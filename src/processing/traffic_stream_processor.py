"""
Robust Kafka -> Spark Structured Streaming -> Parquet processor.

Designed for the TrafficAnalytics project:
- Kafka bootstrap: kafka:9092 (inside Docker Compose)
- Topic: traffic-events
- Parquet output: /opt/traffic/data/processed/traffic_summary

This version intentionally avoids a stateful event-time window aggregation in the
streaming plan. Each micro-batch is aggregated by road and appended to Parquet.
It therefore does not depend on a watermark advancing before rows are written.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg, col, count, current_timestamp, lit, min as spark_min,
    round as spark_round, sum as spark_sum, to_timestamp, when
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, BooleanType
)
from pyspark.sql.utils import AnalysisException
import logging
import os
import sys
import traceback
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_LOCAL_OUTPUT = str(PROJECT_ROOT / "data" / "processed" / "traffic_summary")
DEFAULT_LOCAL_CHECKPOINT = str(PROJECT_ROOT / "data" / "checkpoints" / "traffic-parquet")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "traffic-events")
# Inside Docker Compose the shared path is /opt/traffic/...; for local runs
# without Docker, fall back to the project-relative directory so the
# Streamlit dashboard (which reads the project-relative path by default)
# can find the output.
OUTPUT_PATH = os.getenv(
    "TRAFFIC_PARQUET_PATH",
    "/opt/traffic/data/processed/traffic_summary"
    if Path("/opt/traffic").exists()
    else DEFAULT_LOCAL_OUTPUT,
)
CHECKPOINT_PATH = os.getenv(
    "TRAFFIC_CHECKPOINT_PATH",
    "/tmp/traffic-parquet-checkpoint-fixed"
    if Path("/tmp").exists()
    else DEFAULT_LOCAL_CHECKPOINT,
)
SPARK_MASTER = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
TRIGGER_SECONDS = int(os.getenv("TRAFFIC_TRIGGER_SECONDS", "10"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("TrafficAnalytics")

schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("road_id", StringType(), True),
    StructField("road_name", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("average_speed", DoubleType(), True),
    StructField("free_flow_speed", DoubleType(), True),
    StructField("vehicle_count", IntegerType(), True),
    StructField("accident_reported", BooleanType(), True),
])

spark = (
    SparkSession.builder
    .appName("RealTimeTrafficAnalyticsFixed")
    .master(SPARK_MASTER)
    .config("spark.sql.shuffle.partitions", "6")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

def process_batch(batch_df, batch_id):
    """Aggregate only rows in this micro-batch and append a small Parquet file."""
    try:
        if batch_df.rdd.isEmpty():
            log.info("Batch %s: no Kafka events", batch_id)
            return

        # Keep only successfully parsed records with required numeric fields.
        valid = (
            batch_df
            .withColumn("event_time", to_timestamp(col("timestamp")))
            .filter(
                col("road_id").isNotNull()
                & col("road_name").isNotNull()
                & col("average_speed").isNotNull()
                & col("free_flow_speed").isNotNull()
                & col("vehicle_count").isNotNull()
            )
        )

        if valid.rdd.isEmpty():
            log.warning(
                "Batch %s contained Kafka records, but none passed validation. "
                "Check JSON schema and timestamp format.", batch_id
            )
            return

        # Batch aggregation means the Parquet sink can append immediately;
        # no event-time watermark/window finalization is required.
        summary = (
            valid.groupBy("road_id", "road_name")
            .agg(
                spark_min("event_time").alias("_batch_start"),
                avg("latitude").alias("latitude"),
                avg("longitude").alias("longitude"),
                avg("average_speed").alias("avg_speed"),
                avg("free_flow_speed").alias("avg_free_flow_speed"),
                spark_sum("vehicle_count").alias("total_vehicles"),
                count("*").alias("event_count"),
                spark_sum(
                    when(col("accident_reported") == True, 1).otherwise(0)
                ).alias("accident_count"),
            )
            .withColumn(
                "window_start",
                col("_batch_start").cast("timestamp")
            )
            .withColumn(
                "window_end",
                current_timestamp()
            )
            .withColumn(
                "speed_ratio",
                col("avg_speed") / col("avg_free_flow_speed")
            )
            .withColumn(
                "congestion_percentage",
                spark_round((lit(1.0) - col("speed_ratio")) * 100, 2)
            )
            .withColumn(
                "congestion_level",
                when(col("speed_ratio") >= 0.70, "FREE_FLOW")
                .when(col("speed_ratio") >= 0.40, "MODERATE")
                .when(col("speed_ratio") >= 0.20, "HEAVY")
                .otherwise("SEVERE")
            )
            .select(
                "window_start",
                "window_end",
                "road_id",
                "road_name",
                "latitude",
                "longitude",
                spark_round("avg_speed", 2).alias("avg_speed_kmh"),
                spark_round("avg_free_flow_speed", 2).alias("free_flow_speed_kmh"),
                "speed_ratio",
                "congestion_percentage",
                "congestion_level",
                "total_vehicles",
                "event_count",
                "accident_count",
            )
        )

        row_count = summary.count()
        if row_count:
            (
                summary.coalesce(1)
                .write
                .mode("append")
                .parquet(OUTPUT_PATH)
            )
        log.info(
            "Batch %s: wrote %s road summaries to %s",
            batch_id, row_count, OUTPUT_PATH
        )
    except Exception:
        log.error("Batch %s failed:\n%s", batch_id, traceback.format_exc())
        raise

try:
    log.info("Starting traffic stream processor")
    log.info("Spark master: %s", SPARK_MASTER)
    log.info("Kafka: %s | topic: %s", KAFKA_BOOTSTRAP, KAFKA_TOPIC)
    log.info("Parquet output: %s", OUTPUT_PATH)
    log.info("Checkpoint: %s", CHECKPOINT_PATH)

    Path(CHECKPOINT_PATH).mkdir(parents=True, exist_ok=True)

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    events = (
        raw_stream
        .selectExpr("CAST(value AS STRING) AS json")
        .selectExpr("from_json(json, 'event_id STRING, timestamp STRING, road_id STRING, road_name STRING, latitude DOUBLE, longitude DOUBLE, average_speed DOUBLE, free_flow_speed DOUBLE, vehicle_count INT, accident_reported BOOLEAN') AS event")
        .select("event.*")
    )

    query = (
        events.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
        .start()
    )

    log.info("STREAMING ANALYTICS RUNNING — waiting for Kafka events")
    query.awaitTermination()

except KeyboardInterrupt:
    log.info("Stop requested; shutting down gracefully")
except Exception:
    log.error("Streaming processor failed:\n%s", traceback.format_exc())
    raise
finally:
    spark.stop()

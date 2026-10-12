from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    when,
    window,
    avg,
    sum,
    count,
    round
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    BooleanType,
    TimestampType
)

# ============================================================
# SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("RealTimeTrafficAnalytics")
    .master("spark://spark-master:7077")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("\n" + "=" * 75)
print(" REAL-TIME TRAFFIC CONGESTION & ACCIDENT ANALYTICS")
print("=" * 75)
print("Spark Structured Streaming started successfully.")
print("Kafka Topic: traffic-events")
print("=" * 75 + "\n")


# ============================================================
# KAFKA MESSAGE SCHEMA
# ============================================================

schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("road_id", StringType(), True),
    StructField("road_name", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("average_speed", DoubleType(), True),
    StructField("free_flow_speed", IntegerType(), True),
    StructField("vehicle_count", IntegerType(), True),
    StructField("accident_reported", BooleanType(), True)
])


# ============================================================
# READ DATA FROM KAFKA
# ============================================================

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "traffic-events")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)


# ============================================================
# PARSE JSON
# ============================================================

traffic_df = (
    kafka_df
    .selectExpr("CAST(value AS STRING) AS json_value")
    .select(
        from_json(
            col("json_value"),
            schema
        ).alias("data")
    )
    .select("data.*")
)


# ============================================================
# CONVERT TIMESTAMP
# ============================================================

traffic_df = traffic_df.withColumn(
    "event_time",
    col("timestamp").cast(TimestampType())
)
traffic_df = traffic_df.withWatermark(
    "event_time",
    "10 seconds"
)


# ============================================================
# 30-SECOND WINDOW AGGREGATION
# ============================================================

traffic_summary = (
    traffic_df
    .groupBy(
        window(
            col("event_time"),
            "30 seconds"
        ),
        col("road_id"),
        col("road_name")
    )
    .agg(

        # Average traffic speed
        avg("average_speed").alias("avg_speed"),

        # Average free-flow speed
        avg("free_flow_speed").alias("avg_free_flow_speed"),

        # Total vehicles observed
        sum("vehicle_count").alias("total_vehicles"),

        # Number of traffic events
        count("*").alias("event_count"),

        # Number of accidents
        sum(
            when(
                col("accident_reported") == True,
                1
            ).otherwise(0)
        ).alias("accident_count")
    )
)


# ============================================================
# CALCULATE CONGESTION METRICS
# ============================================================

traffic_summary = traffic_summary.withColumn(

    "speed_ratio",

    col("avg_speed") /
    col("avg_free_flow_speed")
)


traffic_summary = traffic_summary.withColumn(

    "congestion_percentage",

    round(
        (1 - col("speed_ratio")) * 100,
        2
    )
)


# ============================================================
# CONGESTION CLASSIFICATION
# ============================================================

traffic_summary = traffic_summary.withColumn(

    "congestion_level",

    when(
        col("speed_ratio") >= 0.70,
        "FREE_FLOW"
    )
    .when(
        col("speed_ratio") >= 0.40,
        "MODERATE"
    )
    .when(
        col("speed_ratio") >= 0.20,
        "HEAVY"
    )
    .otherwise(
        "SEVERE"
    )
)


# ============================================================
# FINAL OUTPUT COLUMNS
# ============================================================

traffic_summary = (
    traffic_summary
    .select(

        col("window.start").alias("window_start"),

        col("window.end").alias("window_end"),

        col("road_id"),

        col("road_name"),

        round(
            col("avg_speed"),
            2
        ).alias("avg_speed_kmh"),

        round(
            col("avg_free_flow_speed"),
            2
        ).alias("free_flow_speed_kmh"),

        round(
            col("congestion_percentage"),
            2
        ).alias("congestion_percentage"),

        col("congestion_level"),

        col("total_vehicles"),

        col("event_count"),

        col("accident_count")
    )
)


# ============================================================
# OUTPUT 1 — CONSOLE
# ============================================================

console_query = (
    traffic_summary
    .writeStream
    .outputMode("complete")
    .format("console")
    .option("truncate", False)
    .option("numRows", 50)
    .option(
        "checkpointLocation",
        "/tmp/traffic-console-checkpoint"
    )
    .trigger(
        processingTime="10 seconds"
    )
    .start()
)


# ============================================================
# OUTPUT 2 — PARQUET
# ============================================================

parquet_query = (
    traffic_summary
    .writeStream
    .outputMode("append")
    .format("parquet")
    .option(
        "path",
        "/opt/traffic/data/processed/traffic_summary"
    )
    .option(
        "checkpointLocation",
        "/tmp/traffic-parquet-checkpoint"
    )
    .trigger(
        processingTime="10 seconds"
    )
    .start()
)


# ============================================================
# WAIT FOR STREAMING
# ============================================================

print("\n")
print("=" * 75)
print(" STREAMING ANALYTICS RUNNING")
print("=" * 75)
print(" Console output      : ENABLED")
print(" Parquet output      : ENABLED")
print(" Output directory    : /opt/traffic/data/processed/traffic_summary")
print(" Window              : 30 seconds")
print(" Trigger             : 10 seconds")
print("=" * 75)
print("\n")

spark.streams.awaitAnyTermination()
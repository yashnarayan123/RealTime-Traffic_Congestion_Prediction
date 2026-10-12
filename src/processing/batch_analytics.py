"""Local batch analytics helpers (pandas-based, clearly labeled as local).

Scope note: this module is a *simulation/demonstration* of MapReduce-style
aggregation using plain Python — it does not run on Hadoop. The production
streaming path remains: Kafka -> Spark Structured Streaming -> Parquet.
"""
from collections import defaultdict

import pandas as pd

STREAM_REQUIRED_COLUMNS = [
    "road_id",
    "road_name",
    "avg_speed_kmh",
    "congestion_percentage",
    "congestion_level",
    "total_vehicles",
    "event_count",
    "accident_count",
]


def validate_stream_schema(df):
    """Return {'missing_columns': [...], 'null_counts': {...}} for a stream summary frame."""
    missing = [c for c in STREAM_REQUIRED_COLUMNS if c not in df.columns]
    null_counts = (
        {c: int(df[c].isna().sum()) for c in df.columns} if not df.empty else {}
    )
    return {"missing_columns": missing, "null_counts": null_counts}


def sensor_summary(df):
    """Per-road summary: rows, vehicles, avg speed, accidents, worst congestion."""
    if df.empty:
        return pd.DataFrame()
    grouped = (
        df.groupby(["road_id", "road_name"], as_index=False)
        .agg(
            records=("road_id", "size"),
            total_vehicles=("total_vehicles", "sum"),
            avg_speed_kmh=("avg_speed_kmh", "mean"),
            total_accidents=("accident_count", "sum"),
            max_congestion_pct=("congestion_percentage", "max"),
            distinct_windows=("window_start", "nunique")
            if "window_start" in df.columns
            else ("road_id", "size"),
        )
        .sort_values("total_vehicles", ascending=False)
        .reset_index(drop=True)
    )
    return grouped


def pipeline_stats(df, processing_seconds=None):
    """Counts, distinct sensors, throughput for the streaming output."""
    stats = {
        "processed_records": int(len(df)),
        "distinct_sensors": int(df["road_id"].nunique()) if "road_id" in df.columns else 0,
        "total_vehicles": int(df["total_vehicles"].sum())
        if "total_vehicles" in df.columns and not df.empty
        else 0,
        "total_accidents": int(df["accident_count"].sum())
        if "accident_count" in df.columns and not df.empty
        else 0,
        "missing_values": int(df.isna().sum().sum()),
    }
    if processing_seconds:
        stats["throughput_records_per_sec"] = (
            stats["processed_records"] / processing_seconds
            if processing_seconds > 0
            else 0.0
        )
    return stats


def mapreduce_vehicle_count_by_road(records):
    """Simulated MapReduce (local, single-process) vehicle totals per road.

    Map: emit (road_id, vehicle_count) per record.
    Shuffle/sort: group by road_id.
    Reduce: sum vehicle counts.
    """
    mapped = [(r.get("road_id"), r.get("vehicle_count", 0) or 0) for r in records]
    shuffled = defaultdict(list)
    for key, value in mapped:
        if key is None:
            continue
        shuffled[key].append(value)
    return {key: sum(values) for key, values in shuffled.items()}


def congestion_level(avg_speed, free_flow_speed):
    """Mirror of the Spark processor congestion classification (pure Python)."""
    if not free_flow_speed:
        return "UNKNOWN"
    ratio = avg_speed / free_flow_speed
    if ratio >= 0.70:
        return "FREE_FLOW"
    if ratio >= 0.40:
        return "MODERATE"
    if ratio >= 0.20:
        return "HEAVY"
    return "SEVERE"

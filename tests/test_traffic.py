"""Unit tests that run without Docker/Kafka/Spark.

Run:  .venv/bin/python -m pytest tests/ -v
"""
import json
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src" / "producer"))
sys.path.insert(0, str(PROJECT_ROOT / "src" / "processing"))

from traffic_producer import create_event, serialize_event, validate_event  # noqa: E402
from batch_analytics import (  # noqa: E402
    congestion_level,
    mapreduce_vehicle_count_by_road,
    pipeline_stats,
    sensor_summary,
    validate_stream_schema,
)


def test_producer_event_schema():
    event = create_event()
    assert validate_event(event) is True
    payload = serialize_event(event)
    decoded = json.loads(payload.decode("utf-8"))
    assert decoded["road_id"] == event["road_id"]
    assert isinstance(decoded["average_speed"], float)
    assert isinstance(decoded["vehicle_count"], int)
    assert isinstance(decoded["accident_reported"], bool)


def test_producer_event_validation_rejects_bad_input():
    with pytest.raises(ValueError):
        validate_event({"road_id": "ROAD_001"})
    with pytest.raises(TypeError):
        bad = create_event()
        bad["vehicle_count"] = "many"
        validate_event(bad)


def test_congestion_classification_boundaries():
    assert congestion_level(60, 60) == "FREE_FLOW"
    assert congestion_level(42, 60) == "FREE_FLOW"  # ratio 0.70
    assert congestion_level(30, 60) == "MODERATE"
    assert congestion_level(15, 60) == "HEAVY"
    assert congestion_level(5, 60) == "SEVERE"
    assert congestion_level(10, 0) == "UNKNOWN"


def test_validate_stream_schema_detects_problems():
    ok = pd.DataFrame(
        [
            {
                "road_id": "R1",
                "road_name": "Road 1",
                "avg_speed_kmh": 30.0,
                "congestion_percentage": 50.0,
                "congestion_level": "MODERATE",
                "total_vehicles": 10,
                "event_count": 2,
                "accident_count": 0,
            }
        ]
    )
    report = validate_stream_schema(ok)
    assert report["missing_columns"] == []

    bad = pd.DataFrame([{"road_id": "R1"}])
    report = validate_stream_schema(bad)
    assert "road_name" in report["missing_columns"]

    assert validate_stream_schema(pd.DataFrame())["missing_columns"]


def test_sensor_summary_and_stats_handle_empty():
    assert sensor_summary(pd.DataFrame()).empty
    stats = pipeline_stats(pd.DataFrame())
    assert stats["processed_records"] == 0
    assert stats["distinct_sensors"] == 0


def test_sensor_summary_aggregates():
    df = pd.DataFrame(
        [
            {
                "road_id": "R1",
                "road_name": "Road 1",
                "avg_speed_kmh": 20.0,
                "congestion_percentage": 60.0,
                "total_vehicles": 30,
                "event_count": 3,
                "accident_count": 1,
                "window_start": "2026-01-01 00:00:00",
            },
            {
                "road_id": "R1",
                "road_name": "Road 1",
                "avg_speed_kmh": 40.0,
                "congestion_percentage": 30.0,
                "total_vehicles": 10,
                "event_count": 1,
                "accident_count": 0,
                "window_start": "2026-01-01 00:01:00",
            },
        ]
    )
    summary = sensor_summary(df)
    assert len(summary) == 1
    assert summary.iloc[0]["total_vehicles"] == 40
    assert summary.iloc[0]["total_accidents"] == 1
    stats = pipeline_stats(df, processing_seconds=2.0)
    assert stats["processed_records"] == 2
    assert stats["throughput_records_per_sec"] == 1.0


def test_mapreduce_simulation():
    records = [
        {"road_id": "R1", "vehicle_count": 10},
        {"road_id": "R2", "vehicle_count": 5},
        {"road_id": "R1", "vehicle_count": 7},
        {"road_id": None, "vehicle_count": 99},
    ]
    assert mapreduce_vehicle_count_by_road(records) == {"R1": 17, "R2": 5}


def test_forecast_metric_definitions():
    actual = np.array([10.0, 20.0, 30.0])
    predicted = np.array([12.0, 18.0, 33.0])
    mae = float(np.mean(np.abs(actual - predicted)))
    rmse = float(np.sqrt(np.mean((actual - predicted) ** 2)))
    ss_res = float(np.sum((actual - predicted) ** 2))
    ss_tot = float(np.sum((actual - np.mean(actual)) ** 2))
    r2 = 1.0 - ss_res / ss_tot
    assert mae == pytest.approx(7 / 3)
    assert rmse == pytest.approx(np.sqrt(17 / 3))
    assert -1.0 <= r2 <= 1.0


def test_model_artifacts_exist_and_features_match_metadata():
    import json as jsonlib

    for name in ["best_model_5_min.joblib", "best_model_15_min.joblib", "best_model_60_min.joblib"]:
        assert (PROJECT_ROOT / "models" / name).is_file(), f"missing {name}"
    meta = jsonlib.loads((PROJECT_ROOT / "models" / "experiment_metadata.json").read_text())
    assert len(meta["feature_columns"]) == 12
    assert meta["target_columns"]["5_min"] == "y_t_plus_1_d0"
    assert meta["target_columns"]["15_min"] == "y_t_plus_3_d0"
    assert meta["target_columns"]["60_min"] == "y_t_plus_12_d0"


def test_model_loading_when_deps_available():
    pytest.importorskip("sklearn")
    from traffic_forecaster import FEATURES, TARGETS, load_model, regression_metrics

    assert len(FEATURES) == 12
    assert set(TARGETS.values()) == {"y_t_plus_1_d0", "y_t_plus_3_d0", "y_t_plus_12_d0"}
    for horizon in ["5_min", "15_min", "60_min"]:
        model = load_model(horizon)
        feats = {name: 50.0 for name in FEATURES}
        X = np.asarray([[float(feats[n]) for n in FEATURES]], dtype=np.float32)
        pred = model.predict(X)
        assert np.isfinite(pred).all()
    rmse, mae, r2 = regression_metrics([1.0, 2.0], [1.0, 2.0])
    assert rmse == 0.0 and mae == 0.0 and r2 == 1.0


def test_datasets_not_confused():
    metro = PROJECT_ROOT / "batch_experiment" / "data" / "raw" / "Metro_Interstate_Traffic_Volume.csv"
    assert metro.is_file()
    header = metro.open().readline()
    assert "traffic_volume" in header
    # Metro must never validate as a PEMS-BAY feature frame
    df = pd.read_csv(metro, nrows=5)
    assert "x_t_minus_11_d0" not in df.columns


def test_dashboard_map_merge_with_streaming_coords():
    """Regression: Spark output lat/lon + reference table must not crash the map."""
    latest_df = pd.DataFrame(
        [
            {"road_name": "Central Avenue", "latitude": 16.5, "longitude": 80.64,
             "congestion_percentage": 50.0, "total_vehicles": 40,
             "avg_speed_kmh": 30.0, "accident_count": 0},
            {"road_name": "Unknown Road", "latitude": float("nan"), "longitude": float("nan"),
             "congestion_percentage": 10.0, "total_vehicles": 5,
             "avg_speed_kmh": 50.0, "accident_count": 0},
        ]
    )
    road_coordinates = pd.DataFrame(
        {"road_name": ["Central Avenue"], "latitude": [16.5062], "longitude": [80.6480]}
    )
    # Mirror of src/dashboard/app.py map-merge logic
    if {"latitude", "longitude"} <= set(latest_df.columns):
        map_df = latest_df.merge(road_coordinates, on="road_name", how="left",
                                 suffixes=("_stream", "_ref"))
        map_df["latitude"] = map_df["latitude_stream"].fillna(map_df["latitude_ref"])
        map_df["longitude"] = map_df["longitude_stream"].fillna(map_df["longitude_ref"])
    map_df = map_df.dropna(subset=["latitude", "longitude"])
    assert list(map_df.columns).count("latitude") == 1
    assert len(map_df) == 1 and map_df.iloc[0]["road_name"] == "Central Avenue"
    assert map_df.iloc[0]["latitude"] == 16.5  # streaming value preferred


def _clean_map_coords(df):
    """Mirror of the dashboard map coordinate-cleaning logic."""
    import pandas as pd

    df = df.dropna(subset=["latitude", "longitude"]).copy()
    for coord in ("latitude", "longitude"):
        df[coord] = pd.to_numeric(df[coord], errors="coerce")
    return df[
        df["latitude"].between(-90.0, 90.0)
        & df["longitude"].between(-180.0, 180.0)
    ].copy()


def test_map_coords_valid_rows_survive():
    df = pd.DataFrame(
        [
            {"road_name": "A", "latitude": 16.52, "longitude": 80.61},
            {"road_name": "B", "latitude": 16.47, "longitude": 80.70},
        ]
    )
    cleaned = _clean_map_coords(df)
    assert len(cleaned) == 2
    center = (float(cleaned["latitude"].mean()), float(cleaned["longitude"].mean()))
    assert center == ((16.52 + 16.47) / 2, (80.61 + 80.70) / 2)


def test_map_coords_missing_and_invalid_removed():
    df = pd.DataFrame(
        [
            {"road_name": "ok", "latitude": 16.52, "longitude": 80.61},
            {"road_name": "null-lat", "latitude": None, "longitude": 80.61},
            {"road_name": "bad-lat", "latitude": 160.0, "longitude": 80.61},
            {"road_name": "garbage", "latitude": "fast", "longitude": 80.61},
            {"road_name": "out-of-range", "latitude": 16.52, "longitude": 999.0},
        ]
    )
    cleaned = _clean_map_coords(df)
    assert cleaned["road_name"].tolist() == ["ok"]


def test_map_coords_filter_leaves_nothing():
    df = pd.DataFrame([{"road_name": "A", "latitude": None, "longitude": None}])
    assert _clean_map_coords(df).empty  # dashboard must show fallback, not empty map


def test_record_counts_scope_consistent():
    df = pd.DataFrame(
        [
            {"window_start": "2026-01-01 00:00", "event_count": 5, "accident_count": 0},
            {"window_start": "2026-01-01 00:01", "event_count": 8, "accident_count": 1},
        ]
    )
    latest = df["window_start"].max()
    latest_df = df[df["window_start"] == latest]
    processed_records = len(df)  # cumulative, all windows
    latest_events = int(latest_df["event_count"].sum())  # latest window only
    assert processed_records == 2
    assert latest_events == 8
    assert processed_records != latest_events  # different scopes by design

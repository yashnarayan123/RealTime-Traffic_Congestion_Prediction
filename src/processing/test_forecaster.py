"""Smoke-check forecaster: loads each horizon model and scores the PEMS-BAY sample.

Looks for ``pems_bay_prediction_test.csv`` in (in order):
  1. ``$TRAFFIC_DATA_DIR``
  2. ``<project>/data/raw/``
  3. ``~/Downloads/`` (legacy location)

Run:  python src/processing/test_forecaster.py
"""
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

from traffic_forecaster import FEATURES, HORIZONS, TARGETS, load_model  # noqa: E402

PROJECT_ROOT = Path(__file__).resolve().parents[2]

CANDIDATES = [
    Path(os.getenv("TRAFFIC_DATA_DIR", str(PROJECT_ROOT / "data" / "raw")))
    / "pems_bay_prediction_test.csv",
    PROJECT_ROOT / "data" / "raw" / "pems_bay_prediction_test.csv",
    Path.home() / "Downloads" / "pems_bay_prediction_test.csv",
]

CSV_PATH = next((p for p in CANDIDATES if p.is_file()), None)
if CSV_PATH is None:
    print("PEMS-BAY test sample not found. Searched:")
    for candidate in CANDIDATES:
        print(f"  - {candidate}")
    print("Place pems_bay_prediction_test.csv in data/raw/ and retry.")
    raise SystemExit(2)

df = pd.read_csv(CSV_PATH)

missing = [col for col in FEATURES if col not in df.columns]
if missing:
    raise ValueError(f"Missing model features: {missing}")

print(f"Loaded test rows: {len(df)} from {CSV_PATH}")

for horizon, target_col in TARGETS.items():
    model = load_model(horizon)

    X = df[FEATURES].to_numpy(dtype=np.float32)
    actual = df[target_col].to_numpy(dtype=np.float32)
    predicted = model.predict(X)

    mae = float(np.mean(np.abs(actual - predicted)))
    rmse = float(np.sqrt(np.mean((actual - predicted) ** 2)))
    ss_res = float(np.sum((actual - predicted) ** 2))
    ss_tot = float(np.sum((actual - np.mean(actual)) ** 2))
    r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else float("nan")

    comparison = pd.DataFrame({
        "actual": actual,
        "predicted": predicted,
    })

    print(f"\n{horizon} -> {target_col}: MAE={mae:.4f} RMSE={rmse:.4f} R2={r2:.4f}")
    print(comparison.head(5).round(3).to_string(index=False))

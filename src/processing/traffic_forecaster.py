
from pathlib import Path

import joblib
import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parents[2]
MODEL_DIR = PROJECT_ROOT / "models"

FEATURES = (
    [f"x_t_minus_{i}_d0" for i in range(11, 0, -1)]
    + ["x_t_plus_0_d0"]
)

HORIZONS = {
    "5_min": "best_model_5_min.joblib",
    "15_min": "best_model_15_min.joblib",
    "60_min": "best_model_60_min.joblib",
}

TARGETS = {
    "5_min": "y_t_plus_1_d0",
    "15_min": "y_t_plus_3_d0",
    "60_min": "y_t_plus_12_d0",
}


def load_model(horizon="5_min"):
    if horizon not in HORIZONS:
        raise ValueError(f"Unsupported horizon: {horizon}")

    model_path = MODEL_DIR / HORIZONS[horizon]

    if not model_path.exists():
        raise FileNotFoundError(
            f"Model not found: {model_path}. "
            "Expected trained PEMS-BAY artifacts in <project>/models/."
        )

    try:
        return joblib.load(model_path)
    except Exception as exc:
        raise RuntimeError(
            f"Failed to load {model_path} (trained with scikit-learn 1.6.1; "
            f"check that scikit-learn/numpy versions are compatible): {exc}"
        ) from exc


def regression_metrics(actual, predicted):
    """Return (rmse, mae, r2) computed with the standard definitions."""
    actual = np.asarray(actual, dtype=np.float64)
    predicted = np.asarray(predicted, dtype=np.float64)
    mae = float(np.mean(np.abs(actual - predicted)))
    rmse = float(np.sqrt(np.mean((actual - predicted) ** 2)))
    ss_res = float(np.sum((actual - predicted) ** 2))
    ss_tot = float(np.sum((actual - np.mean(actual)) ** 2))
    r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else float("nan")
    return rmse, mae, r2


def predict(model, features):
    missing = [name for name in FEATURES if name not in features]
    if missing:
        raise ValueError(f"Missing features: {missing}")

    X = np.asarray(
        [[float(features[name]) for name in FEATURES]],
        dtype=np.float32,
    )

    if not np.isfinite(X).all():
        raise ValueError("All feature values must be finite numbers.")

    return float(model.predict(X)[0])


if __name__ == "__main__":
    print("Project:", PROJECT_ROOT)
    print("Model directory:", MODEL_DIR)

    for horizon in HORIZONS:
        print(f"\nLoading {horizon} model...")
        model = load_model(horizon)
        print("Loaded:", type(model).__name__)

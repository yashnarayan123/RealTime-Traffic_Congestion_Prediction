
from pathlib import Path

import os

import joblib
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

# ============================================================
# PATHS AND CONFIGURATION
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[3]
MODEL_DIR = PROJECT_ROOT / "models"

# PEMS-BAY test sample lookup (Linux-compatible).
# Priority: project-local data/raw -> DATA_DIR env -> legacy ~/Downloads.
# NOTE: This is historical PEMS-BAY (San Francisco Bay Area) sensor data and
# is intentionally separate from the Metro Interstate dataset in
# batch_experiment/data/raw/.
DATA_DIR = Path(os.getenv("TRAFFIC_DATA_DIR", str(PROJECT_ROOT / "data" / "raw")))

CANDIDATE_CSVS = [
    DATA_DIR / "pems_bay_prediction_test.csv",
    PROJECT_ROOT / "data" / "raw" / "pems_bay_prediction_test.csv",
    MODEL_DIR / "pems_bay_prediction_test.csv",
    Path.home() / "Downloads" / "pems_bay_prediction_test.csv",
]

SAMPLE_CSV = next((p for p in CANDIDATE_CSVS if p.is_file()), CANDIDATE_CSVS[0])
RESULTS_CSV = MODEL_DIR / "model_comparison.csv"

FEATURES = (
    [f"x_t_minus_{i}_d0" for i in range(11, 0, -1)]
    + ["x_t_plus_0_d0"]
)

HORIZONS = {
    "5 minutes": {
        "key": "5_min",
        "model": "best_model_5_min.joblib",
        "target": "y_t_plus_1_d0",
    },
    "15 minutes": {
        "key": "15_min",
        "model": "best_model_15_min.joblib",
        "target": "y_t_plus_3_d0",
    },
    "60 minutes": {
        "key": "60_min",
        "model": "best_model_60_min.joblib",
        "target": "y_t_plus_12_d0",
    },
}

# ============================================================
# PAGE CONFIGURATION
# ============================================================

st.set_page_config(
    page_title="Traffic Forecasting",
    page_icon="📈",
    layout="wide",
)

st.title("📈 Traffic Forecasting")
st.caption(
    "Historical traffic forecasting using trained PEMS-BAY models"
)

st.info(
    "Data source: PEMS-BAY historical traffic data from the "
    "San Francisco Bay Area. These predictions are separate from "
    "the simulated traffic events in the live analytics dashboard."
)

# ============================================================
# VALIDATE REQUIRED FILES
# ============================================================

required_models = [
    MODEL_DIR / info["model"]
    for info in HORIZONS.values()
]

missing_models = [str(p) for p in required_models if not p.is_file()]

if missing_models:
    st.error("The following model files are missing:")
    for path in missing_models:
        st.code(path)
    st.stop()

if not SAMPLE_CSV.is_file():
    st.warning(
        "The PEMS-BAY test sample CSV was not found."
    )
    st.write("Searched locations:")
    for candidate in CANDIDATE_CSVS:
        st.code(str(candidate))
    st.write(
        "Place `pems_bay_prediction_test.csv` in `data/raw/` "
        "(preferred on Linux) or set `TRAFFIC_DATA_DIR` to the folder "
        "containing it. Do NOT substitute the Metro Interstate CSV: "
        "it has different columns (`traffic_volume`, weather, etc.) and "
        "is a separate dataset in `batch_experiment/data/raw/`."
    )
    st.stop()

# ============================================================
# LOAD DATA AND MODELS
# ============================================================

@st.cache_data
def load_sample(path, modified_time):
    data = pd.read_csv(path)

    missing = [
        column
        for column in FEATURES
        + [info["target"] for info in HORIZONS.values()]
        if column not in data.columns
    ]

    if missing:
        raise ValueError(f"CSV is missing columns: {missing}")

    for column in FEATURES + [
        info["target"] for info in HORIZONS.values()
    ]:
        data[column] = pd.to_numeric(data[column], errors="coerce")

    data = data.replace([np.inf, -np.inf], np.nan)
    data = data.dropna(
        subset=FEATURES
        + [info["target"] for info in HORIZONS.values()]
    )

    return data.reset_index(drop=True)


@st.cache_resource
def load_model(model_path, modified_time):
    return joblib.load(model_path)


try:
    df = load_sample(str(SAMPLE_CSV), SAMPLE_CSV.stat().st_mtime)
except Exception as exc:
    st.error(f"Unable to load the PEMS-BAY sample: {exc}")
    st.stop()

if df.empty:
    st.error("The CSV contains no usable rows.")
    st.stop()

# ============================================================
# SIDEBAR CONTROLS
# ============================================================

st.sidebar.header("Forecast Controls")

horizon_label = st.sidebar.selectbox(
    "Forecast horizon",
    list(HORIZONS.keys()),
    index=0,
)

horizon = HORIZONS[horizon_label]
model_path = MODEL_DIR / horizon["model"]

row_limit = st.sidebar.slider(
    "Rows to display",
    min_value=10,
    max_value=len(df),
    value=min(100, len(df)),
    step=10 if len(df) >= 10 else 1,
)

# ============================================================
# GENERATE PREDICTIONS
# ============================================================

try:
    model = load_model(str(model_path), model_path.stat().st_mtime)

    X = df[FEATURES].to_numpy(dtype=np.float32)
    actual = df[horizon["target"]].to_numpy(dtype=np.float32)
    predicted = model.predict(X)

except Exception as exc:
    st.error(f"Prediction failed: {exc}")
    st.stop()

comparison = pd.DataFrame({
    "Sample row": np.arange(1, len(df) + 1),
    "Actual": actual,
    "Predicted": predicted,
})

comparison["Absolute Error"] = np.abs(
    comparison["Actual"] - comparison["Predicted"]
)

# ============================================================
# FORECAST SUMMARY
# ============================================================

st.subheader(f"{horizon_label} Forecast")

m1, m2, m3, m4 = st.columns(4)

with m1:
    st.metric("Valid sample rows", f"{len(df):,}")

with m2:
    st.metric(
        "Sample MAE",
        f"{comparison['Absolute Error'].mean():.3f}",
    )

with m3:
    st.metric(
        "Sample RMSE",
        f"{np.sqrt(np.mean((actual - predicted) ** 2)):.3f}",
    )

with m4:
    _ss_res = float(np.sum((actual - predicted) ** 2))
    _ss_tot = float(np.sum((actual - np.mean(actual)) ** 2))
    _r2 = 1.0 - _ss_res / _ss_tot if _ss_tot > 0 else float("nan")
    st.metric("Sample R²", f"{_r2:.4f}")

st.caption(
    "The MAE and RMSE above are calculated from the small CSV sample, "
    "not the complete test set. Use the recorded evaluation table below "
    "for the experiment's test-set metrics."
)

# ============================================================
# ACTUAL VS PREDICTED CHART
# ============================================================

st.subheader("Actual vs Predicted Values")

plot_df = comparison.head(row_limit)

fig = go.Figure()

fig.add_trace(
    go.Scatter(
        x=plot_df["Sample row"],
        y=plot_df["Actual"],
        mode="lines+markers",
        name="Actual",
        line=dict(color="#0fd7c4", width=2),
        marker=dict(size=5),
    )
)

fig.add_trace(
    go.Scatter(
        x=plot_df["Sample row"],
        y=plot_df["Predicted"],
        mode="lines+markers",
        name="Predicted",
        line=dict(color="#a78bfa", width=2),
        marker=dict(size=5),
    )
)

fig.update_layout(
    template="plotly_dark",
    paper_bgcolor="#06091a",
    plot_bgcolor="#0b1120",
    height=450,
    margin=dict(l=20, r=20, t=30, b=20),
    xaxis_title="Sample row",
    yaxis_title="Traffic target (dataset units)",
    legend=dict(orientation="h", y=1.12),
)

st.plotly_chart(fig, use_container_width=True)

# ============================================================
# RECORDED EXPERIMENT METRICS
# ============================================================

st.subheader("Recorded Model Evaluation")

if RESULTS_CSV.is_file():
    try:
        results = pd.read_csv(RESULTS_CSV)

        results = results[
            (results["horizon"] == horizon["key"])
            & (results["split"] == "test")
        ].copy()

        if not results.empty:
            results = results.sort_values("RMSE")

            st.caption(
                "These are the previously recorded test metrics "
                "from the sampled experiment."
            )

            st.dataframe(
                results[["model", "RMSE", "MAE", "R2"]].rename(
                    columns={
                        "model": "Model",
                        "RMSE": "Test RMSE",
                        "MAE": "Test MAE",
                        "R2": "Test R²",
                    }
                ).round(4),
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.warning(
                f"No recorded test metrics found for {horizon['key']}."
            )

    except Exception as exc:
        st.warning(f"Could not read recorded metrics: {exc}")
else:
    st.warning(
        "model_comparison.csv was not found in the models folder."
    )

# ============================================================
# SAMPLE TABLE AND DOWNLOAD
# ============================================================

st.subheader("Prediction Details")

st.dataframe(
    comparison.head(row_limit).round(4),
    use_container_width=True,
    hide_index=True,
)

st.download_button(
    label="Download displayed predictions as CSV",
    data=comparison.head(row_limit).to_csv(index=False).encode("utf-8"),
    file_name=f"pems_bay_predictions_{horizon['key']}.csv",
    mime="text/csv",
)

st.caption(
    "This page demonstrates predictions on historical PEMS-BAY data. "
    "It does not yet forecast the simulated roads in the live dashboard."
)

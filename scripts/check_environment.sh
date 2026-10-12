#!/usr/bin/env bash
# Environment checks with actionable errors. Never modifies data.
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
FAIL=0

ok()   { echo "OK:   $1"; }
warn() { echo "WARN: $1"; }
bad()  { echo "FAIL: $1"; FAIL=1; }

echo "=== TrafficAnalytics environment check ==="

python3 --version >/dev/null 2>&1 && ok "python3 $(python3 --version 2>&1)" || bad "python3 not found"
[ -d ".venv" ] && ok ".venv exists" || bad "missing .venv (run ./scripts/setup.sh)"
if [ -x ".venv/bin/python" ]; then
  for mod in pandas numpy sklearn streamlit plotly kafka pyspark pyarrow; do
    .venv/bin/python -c "import $mod" 2>/dev/null && ok "py module: $mod" || bad "py module missing: $mod"
  done
  .venv/bin/python - <<'EOF' || true
import sys
sys.path.insert(0, "src/processing")
from pathlib import Path
ok = True
for n in ["best_model_5_min.joblib","best_model_15_min.joblib","best_model_60_min.joblib"]:
    p = Path("models")/n
    print(("OK:   model "+n) if p.is_file() else ("FAIL: model missing "+n))
    ok = ok and p.is_file()
try:
    from traffic_forecaster import load_model
    for h in ["5_min","15_min","60_min"]:
        m = load_model(h); print(f"OK:   loads {h}: {type(m).__name__}")
except Exception as e:
    print(f"WARN: model load check skipped/failed: {e}")
EOF
else
  bad ".venv/bin/python not executable"
fi

[ -f "data/raw/pems_bay_prediction_test.csv" ] && ok "PEMS-BAY sample in data/raw/" \
  || warn "data/raw/pems_bay_prediction_test.csv missing (forecasting page will show guidance)"
[ -f "$HOME/Downloads/pems_bay_prediction_test.csv" ] && ok "legacy sample in ~/Downloads/"
[ -d "data/processed/traffic_summary" ] && ok "parquet output dir exists" \
  || warn "data/processed/traffic_summary missing (created on first streaming run)"

command -v docker >/dev/null 2>&1 && ok "docker $(docker --version 2>&1)" || warn "docker not installed (streaming integration unavailable)"
if command -v docker >/dev/null 2>&1; then
  docker compose version >/dev/null 2>&1 && ok "docker compose available" || warn "docker compose plugin missing"
  docker compose config >/dev/null 2>&1 && ok "docker-compose.yml validates" || bad "docker compose config failed"
fi
command -v java >/dev/null 2>&1 && ok "java $(java -version 2>&1 | head -n1)" || warn "java missing (PySpark needs Java 17 for Spark 4)"

echo "=== done (exit $FAIL) ==="
exit $FAIL

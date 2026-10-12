#!/usr/bin/env bash
# Setup: create project-local .venv and install dependencies (no sudo pip).
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

echo "==> Python: $(python3 --version)"
if [ ! -d ".venv" ]; then
  echo "==> Creating .venv ..."
  python3 -m venv .venv
else
  echo "==> .venv already exists, reusing."
fi
.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install -r requirements.txt

mkdir -p data/raw data/processed/traffic_summary data/checkpoints logs

echo "==> Checking models ..."
ls -la models/
echo ""
echo "Setup complete."
echo "Next: ./scripts/check_environment.sh"

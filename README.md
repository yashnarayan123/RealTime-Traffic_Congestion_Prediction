# TrafficAnalytics_Teammates — Linux Guide

Real-time traffic analytics: Python producer → Kafka → Spark Structured
Streaming → Parquet → Streamlit dashboard, plus PEMS-BAY traffic forecasting
(5 / 15 / 60 min horizons).

## 0. Quick start (single command)

```bash
./scripts/start.sh --full-stack   # docker + topic + processor + producer + dashboard
./scripts/stop.sh                 # stops everything, keeps all data
```

This is the supported way to run the project. `start.sh` refuses duplicates
(every component prints WARN instead of launching twice), and `stop.sh`
stops the processor driver, dashboard, producer and Compose services without
deleting data. URLs: dashboard http://localhost:8501, Spark master
http://localhost:8080, Kafka `localhost:9094` (host) / `kafka:9092` (containers).

## 1. Prerequisites

- Linux with Python 3.11+ (`python3 --version`; 3.14 works — scikit-learn 1.6.1
  builds from source, takes a few minutes)
- Java 17+ for Spark (`java -version`; Spark 4.0.1 does not run on Java 8/11)
- Docker + Docker Compose plugin **only** for the Kafka/Spark streaming path
  (`docker --version`, `docker compose version`)
- ~2 GB free for `.venv` (PySpark + Streamlit); Spark jars download on image build

## 2. Python environment setup

```bash
./scripts/setup.sh        # creates .venv, installs requirements.txt
./scripts/check_environment.sh
```

What `setup.sh` does: `python3 -m venv .venv` (no `sudo pip`, system Python
untouched), upgrades pip, installs `requirements.txt`, creates
`data/raw/ data/processed/traffic_summary/ data/checkpoints/ logs/`.

Key pins: `scikit-learn==1.6.1` (matches the trained `models/*.joblib`),
`pyspark==4.0.1` (matches `Dockerfile.spark` Kafka connector jars).

## 3. Docker startup (streaming path)

```bash
docker build -f Dockerfile.spark -t traffic-spark:4.0.1-kafka .
docker compose up -d kafka spark-master spark-worker
docker compose ps
# or: ./scripts/start.sh --with-docker
```

- Kafka host endpoint (your laptop): `localhost:9094`
- Kafka container endpoint (Spark/producer-in-docker): `kafka:9092`
- Spark master UI: http://localhost:8080 · master URL `spark://localhost:7077`
  from host, `spark://spark-master:7077` from inside containers
- Topic: `traffic-events` (auto-created on first produce; 3 partitions)

## 4. Start the producer

```bash
.venv/bin/python src/producer/traffic_producer.py
# options via env: KAFKA_BOOTSTRAP_SERVERS=localhost:9094 KAFKA_TOPIC=traffic-events
#   TRAFFIC_PRODUCE_INTERVAL_SECONDS=1.0 TRAFFIC_MAX_MESSAGES=100
```

Message schema (producer ↔ Spark `from_json` agree on these names/types):
`event_id, timestamp, road_id, road_name, latitude, longitude,
average_speed, free_flow_speed, vehicle_count, accident_reported`.

## 5. Start the Spark processor

On the spark-master container:

```bash
docker exec traffic-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/traffic/src/processing/traffic_stream_processor.py
```

Env overrides: `KAFKA_BOOTSTRAP_SERVERS` (default `kafka:9092`),
`KAFKA_TOPIC`, `TRAFFIC_PARQUET_PATH`
(default `/opt/traffic/data/processed/traffic_summary` in containers,
project-relative `data/processed/traffic_summary` otherwise),
`TRAFFIC_CHECKPOINT_PATH`, `TRAFFIC_TRIGGER_SECONDS=10`,
`SPARK_MASTER_URL`. Design: per-micro-batch road aggregation appended to
Parquet via `foreachBatch` (no watermark-dependent append sink), so output
appears after the first trigger even with sparse traffic. Restartable via
checkpointing; stopping keeps Parquet data (at-least-once semantics —
replays after a crash can duplicate the in-flight micro-batch).

## 6. Start the dashboard

```bash
.venv/bin/python -m streamlit run src/dashboard/app.py --server.port 8501
# or: ./scripts/start.sh --dashboard-only   (refuses duplicates)
```

- Main dashboard: http://localhost:8501 (works with **no data yet** — shows a
  guided warning instead of crashing)
- Forecasting page: same app → “Traffic Forecasting” page. Needs
  `data/raw/pems_bay_prediction_test.csv` (preferred) — see §8.

## 7. Ports, data, logs

| What | Where |
|---|---|
| Dashboard | http://localhost:8501 |
| Spark master UI / worker UI | http://localhost:8080 / :8081 |
| Kafka (host / container) | `localhost:9094` / `kafka:9092` |
| Streaming Parquet output | `data/processed/traffic_summary/` |
| Checkpoints | `data/checkpoints/` or `/tmp/traffic-parquet-checkpoint-fixed` |
| Logs (`start.sh`) | `logs/dashboard.log`, `logs/producer.log` |
| Metro Interstate dataset | `batch_experiment/data/raw/Metro_Interstate_Traffic_Volume.csv` |
| Historical models + recorded metrics | `models/best_model_{5,15,60}_min.joblib`, `models/model_comparison.csv` |

## 8. Forecasting data (important)

The forecasting page uses **historical PEMS-BAY** sensor data, a different
dataset from the **Metro Interstate** CSV (which has `traffic_volume`,
weather columns — never a substitute). Lookup order:
`$TRAFFIC_DATA_DIR/pems_bay_prediction_test.csv` → `data/raw/…` →
`~/Downloads/…` (legacy). If absent, the page lists searched paths and the
rest of the dashboard keeps working. Do **not** retrain/overwrite
`models/*.joblib` unless you have a justified reason.

Expected PEMS-BAY columns: 12 features
`x_t_minus_11_d0 … x_t_minus_1_d0, x_t_plus_0_d0` and targets
`y_t_plus_1_d0` (5 min), `y_t_plus_3_d0` (15 min), `y_t_plus_12_d0` (60 min).

Live-scored 2026-10-12 on the 100-row sample in `data/raw/`
(single sensor, 8.25 h window) via `test_forecaster.py`:

| Horizon | Sample MAE | Sample RMSE | Sample R² | Recorded test R² (full sampled split) |
|---|---|---|---|---|
| 5 min | 0.738 | 0.927 | −0.10 | 0.966 |
| 15 min | 0.852 | 1.077 | −0.55 | 0.889 |
| 60 min | 1.344 | 1.841 | −3.40 | 0.600 |

MAE/RMSE are genuinely computed and healthy; the negative sample R² is the
correct arithmetic for this near-constant 100-row slice (target std ≈ 0.88 —
residual variance exceeds total variance), not a model failure. Quote the
recorded column for review/viva claims; use the sample column as the live
pipeline proof.

## 9. Run tests

```bash
.venv/bin/python -m pytest tests/ -v
.venv/bin/python src/processing/test_forecaster.py   # needs PEMS-BAY CSV (§8)
./scripts/check_environment.sh
```

Covered: producer serialization/validation, congestion logic, schema/missing/
empty handling, sensor summaries, MapReduce simulation, metric definitions,
model loading + feature/target mapping, dataset distinction. Docker/Kafka
integration tests were **not** run here (no Docker in this environment).

## 10. Stop / clean up safely

```bash
./scripts/stop.sh        # stops dashboard+producer+compose services, keeps data
```

`stop.sh` never deletes data. Note it stops **any** dashboard/producer whose
command line or working directory belongs to this project — including a
foreground `streamlit run` you started manually in this folder. To stop only
background launches, kill the PIDs in `logs/dashboard.pid` / `logs/producer.pid`.
To clear **generated** output only:
`rm -rf data/processed/traffic_summary/* data/checkpoints/*`.
`docker compose down -v` additionally erases queued Kafka messages. Never
delete `models/`, `data/raw/`, or `batch_experiment/data/raw/`.

## 11. Troubleshooting

- `ModuleNotFoundError: sklearn/pandas/streamlit/...` → run `./scripts/setup.sh`
- `pems_bay_prediction_test.csv not found` → place it in `data/raw/` (§8)
- Dashboard empty warning → normal before first streaming run; start
  producer + Spark processor (§4–5)
- `Kafka send failed` → is `docker compose ps` showing kafka healthy?
  Host code must use `localhost:9094`, container code `kafka:9092`
- Spark `AnalysisException` / Java errors → need Java 17+; Spark 4 + Java 27
  was observed working for import but full streaming needs Docker
- `sklearn` load error mentioning versions → keep `scikit-learn==1.6.1`
- Spark `Mkdirs failed ... traffic_summary/_temporary` → the Spark executor
  runs as UID 185 (`spark`) inside the container and writes through the
  `./data` bind mount. Fix: `chmod a+rwX data/processed
  data/processed/traffic_summary data/checkpoints` (host dirs only — done
  automatically by `start.sh --full-stack`). Never `chown`/`sudo` for this.
- Port clash (8501/8080/7077/9094) → stop duplicates (`./scripts/stop.sh`)
  or point `TRAFFIC_PARQUET_PATH`/`SPARK_MASTER_URL` elsewhere
- Dashboard exits immediately when started in background → was Streamlit's
  first-run email prompt blocking on stdin (evidence: `Email:` in
  `logs/dashboard.log`). Fixed: `start.sh` pre-seeds
  `~/.streamlit/credentials.toml`, launches with `< /dev/null`, tracks
  `logs/*.pid`, and matches processes scoped to this project only
- No Docker on this machine → everything except live Kafka→Spark→Parquet
  still works: unit tests, model loading, dashboard (empty-state), analytics

### Install Docker (CachyOS / Arch — run yourself, needs sudo)

```bash
sudo pacman -Syu docker docker-compose
sudo systemctl enable --now docker
sudo usermod -aG docker $USER   # then log out and back in
docker --version && docker compose version
```

After installing, tell the agent and it will guide the live integration test
(§4–5 plus README §9 step 10 in the original task: build image → start Kafka/
Spark → health check → submit processor → bounded produce → verify Parquet →
dashboard reads it).

## 12. What changed (Linux port)

- `src/dashboard/app.py`: `C:\Projects\TrafficAnalytics` → project-root-relative
  `pathlib` path + `$TRAFFIC_PARQUET_PATH` override; empty-state guidance
- `src/dashboard/pages/2_Traffic_Forecasting.py`: `~/Downloads`-only lookup →
  `data/raw/` first with env override + legacy fallback; added sample R²;
  explicit Metro/PEMS-BAY distinction
- `src/producer/traffic_producer.py`: env-configurable bootstrap/topic/interval/
  max-messages, `serialize_event`/`validate_event` helpers, logging, graceful stop
- `src/processing/traffic_stream_processor.py`: env-configurable master/output/
  checkpoint (project-relative fallback outside Docker), checkpoint dir creation
- `src/processing/traffic_forecaster.py`: added `TARGETS` map, `regression_metrics`,
  clearer load errors; `test_forecaster.py` rewritten (fixed broken import +
  Downloads-only path, prints MAE/RMSE/R²)
- New: `src/processing/batch_analytics.py` (sensor summaries, pipeline stats,
  schema checks, clearly-labeled local MapReduce simulation), `tests/`,
  `requirements.txt`, `scripts/{setup,start,stop,check_environment}.sh`,
  `.gitignore`, this README
- Debug pass: `start.sh`/`stop.sh` hardened — credentials pre-seed,
  `< /dev/null` stdin, `logs/*.pid` tracking, project-scoped (args or cwd)
  process matching so other projects are never touched; duplicate launches
  refused and verified. `--full-stack` starts the whole pipeline with one
  command (compose up, Kafka health wait, topic ensure, permissions fix,
  processor submit, producer, dashboard); `stop.sh` also stops the Spark
  driver in-container before `compose stop`
- Live-debugging fixes: dashboard Traffic Map crashed once real Parquet data
  arrived (Spark's `latitude`/`longitude` columns collided with the hardcoded
  reference table → `latitude_x/_y`); the map now coalesces streaming
  coordinates with the reference fallback. Blank-map root cause: an explicit
  `zoom=11` with no `center` left Plotly's viewport at (0, 0) while data sits
  at (16.5, 80.6) — the map now centers on the data mean, pins
  `map_style="open-street-map"`, caps marker size, validates coordinate
  ranges, and shows a fallback message when filters leave no mappable rows.
  Record counts relabeled: banner + summary both show cumulative
  "Processed Rows (All Windows)" from the same snapshot (earlier 529 vs 548
  sightings were two auto-refresh renders seconds apart on a live stream),
  plus a new "Latest-window events" latest-batch metric. Added `.dockerignore` (excludes
  `.venv`, `models`, `data`, `logs`, … — the Spark image needs no context
  files; sources are bind-mounted)

End-to-end verified live (Docker): 60 bounded + continuous producer events →
Kafka `traffic-events` (15+21+24 across 3 partitions) → Spark Structured
Streaming micro-batches → Parquet (`data/processed/traffic_summary`,
7 → 18 → 42 rows across restarts, 5 roads, 0 nulls) → dashboard HTTP 200 with
zero log exceptions. Restart (`compose stop`/`up` + processor resubmit)
resumes from checkpoint with no reprocessing gaps. `pytest` 16/16 pass
including the new map-merge regression test.
**Still not verifiable**: browser rendering (HTTP + AppTest only).

Earlier offline verification: `py_compile` all sources, `pytest` pass, model
artifacts load and predict (sklearn 1.6.1), compose YAML parses, dashboard
serves HTTP 200 + `/_stcore/health ok` with missing Parquet, Streamlit AppTest
proves both missing-data warnings render, Parquet read/analytics round-trip in
temp dirs, producer↔Spark schema key equality, background launch +
duplicate-refusal. Live end-to-end (with Docker) is verified as described
above. Live PEMS-BAY scoring is verified on the 100-row sample (§8).
Remaining gaps: actual browser rendering.

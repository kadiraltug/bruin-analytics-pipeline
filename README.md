# Game Analytics Pipeline

A data platform for a mobile game. A generator creates fake events, Kafka streams them, and Bruin processes everything — from raw ingestion to analytics-ready tables. Streamlit shows the results live.

Everything runs in Docker. I only needed Docker and Docker Compose.

---

## What I built with Bruin

Bruin is the core of this pipeline. Every data movement and transformation is a Bruin asset.

I used three types of Bruin assets:

**1. Ingestr asset** (`kafka_to_pg.asset.yml`) — Reads from Kafka and writes raw JSON into Postgres. No code needed, just a YAML file. Bruin handles Kafka offsets and incremental reads automatically.

**2. Python assets** — I wrote two:

- `game_events_clean.py` — Reads raw JSON from source Postgres, parses it into proper columns, and returns a DataFrame. Bruin uploads the result to `raw.game_events` with merge/upsert.
- `stg_game_events.py` — Reads from the raw table, cleans strings, converts timestamps, removes duplicates, and returns a DataFrame. Bruin uploads to `staging.game_events`.

Both Python assets use `strategy: merge` with `event_id` as primary key, so re-runs are safe (no duplicates). I also enabled `enforce_schema: true` so Bruin uses my column definitions instead of guessing types.

**3. SQL assets** — Three mart tables built from staging:

- `daily_kpis.sql` — DAU, revenue, ARPDAU, ARPPU, sessions per user.
- `level_funnel_daily.sql` — Level completion rates, win/fail breakdown.
- `churn_daily.sql` — D1/D7/D30 retention-based churn per install cohort.

SQL assets use `create+replace` — Bruin drops and recreates the table each run.

**Other Bruin features I used:**

- **Watermarks** — Both Python assets track their progress in `meta.load_state`. Each run only processes new rows.
- **Data quality checks** — The staging asset has `not_null` and `unique` checks on key columns. Bruin runs these after materialization.
- **Connection management** — Kafka, source Postgres, and analytics Postgres are all defined in `.bruin.yml`. Assets reference them by name.
- **Dependency management** — Python dependencies are in `requirements.txt`. Bruin installs them in isolated environments using `uv`.

---

## How the system works

```text
Generator --> Kafka --> Bruin (ingestr) --> Postgres (source)
                                              |
                              Bruin (Python asset: raw layer)
                                              |
                              Bruin (Python asset: staging layer)
                                              |
                              Bruin (SQL assets: marts layer)
                                              |
                                       Streamlit dashboard
```

1. **Generator** creates fake game events and sends them to Kafka.
2. **Bruin ingestr** reads from Kafka every few seconds and writes raw JSON to source Postgres.
3. **Bruin Python asset** (every 1 min via Airflow) parses JSON into columns → `raw.game_events`.
4. **Bruin Python asset** (every 3 min via Airflow) cleans and deduplicates → `staging.game_events`.
5. **Bruin SQL assets** (right after staging) build mart tables.
6. **Streamlit** reads from marts and shows 6 dashboard pages: Overview, Levels, Retention, Churn, Revenue, Pipeline Health.

---

## Services

| Service              | Description                              | Port |
|----------------------|------------------------------------------|------|
| `kafka`              | Message broker                           | 9092 |
| `postgres`           | Source database (raw Kafka data)         | 5432 |
| `postgres-analytics` | Analytics database (raw, staging, marts) | 5433 |
| `bruin`              | Runs all Bruin assets                    | -    |
| `fake-data-generator`| Sends fake events to Kafka               | -    |
| `airflow-webserver`  | Airflow UI                               | 8080 |
| `airflow-scheduler`  | Runs DAGs on schedule                    | -    |
| `streamlit`          | Live dashboards                          | 8501 |

---

## How to run

### 1. Clone

```bash
git clone <repo-url>
cd bruin-endtoend
```

### 2. Create the `.env` file

```bash
cp .env.example .env
```

Here is an example `.env` for local Docker:

```env
POSTGRES_APP_USER=app
POSTGRES_APP_PASSWORD=app
POSTGRES_APP_DB=appdb

POSTGRES_ANALYTICS_USER=analytics
POSTGRES_ANALYTICS_PASSWORD=analytics
POSTGRES_ANALYTICS_DB=analyticsdb

RAW_SOURCE_PG_DSN=postgresql://app:app@postgres:5432/appdb
RAW_SOURCE_PG_TABLE=ingest.game_events_raw
RAW_DEST_PG_DSN=postgresql://analytics:analytics@postgres-analytics:5432/analyticsdb
RAW_LOOKBACK_MS=300000
RAW_ASSET_KEY=raw.game_events

STAGING_DEST_PG_DSN=postgresql://analytics:analytics@postgres-analytics:5432/analyticsdb
STAGING_SOURCE_TABLE=raw.game_events
STAGING_ASSET_KEY=staging.game_events
STAGING_LOOKBACK_MS=300000
STATE_TABLE=meta.load_state

KAFKA_BOOTSTRAP_SERVERS=kafka:29092
KAFKA_TOPIC=game_events
KAFKA_GROUP_ID=game-events-consumer
GENERATOR_RATE_PER_SEC=100

ANALYTICS_PG_DSN=postgresql://analytics:analytics@postgres-analytics:5432/analyticsdb

AIRFLOW_FERNET_KEY=ne7SbTNREThJUH9tsSDHQAFSV7WHrvSpTnzLwS0YtLI=
AIRFLOW_PG_DSN=postgresql://analytics:analytics@postgres-analytics:5432/airflowdb

AWS_EC2_METADATA_DISABLED=true
UV_HTTP_TIMEOUT=1000
```

### 3. Create the Bruin config

```bash
cp bruin/.bruin.yml.example bruin/.bruin.yml
```

Here is an example `.bruin.yml` for local Docker:

```yaml
default_environment: default
environments:
  default:
    connections:
      kafka:
        - name: kafka-default
          bootstrap_servers: "kafka:29092"
          group_id: "game-events-consumer"

      postgres:
        - name: pg-dest
          host: "postgres"
          port: 5432
          database: "appdb"
          username: "app"
          password: "app"

        - name: pg-analytics-dest
          host: "postgres-analytics"
          port: 5432
          database: "analyticsdb"
          username: "analytics"
          password: "analytics"
```

### 4. Start

```bash
docker compose up -d --build
```

Wait 1–2 minutes, then open:

- Dashboard: `http://localhost:8501`
- Airflow: `http://localhost:8080` (admin / admin)

## How to stop

```bash
docker compose down        # keep data
docker compose down -v     # delete everything
```

---

## Useful commands

```bash
# Check row counts
docker exec postgres-analytics psql -U analytics -d analyticsdb -c "SELECT count(*) FROM staging.game_events;"

# Check watermarks
docker exec postgres-analytics psql -U analytics -d analyticsdb -c "SELECT * FROM meta.load_state;"

# View logs
docker logs bruin --tail 50 -f

# Manually run a Bruin asset
docker exec bruin bruin run bruin-pipeline/assets/marts/daily_kpis.sql
```

---

## Tech stack

- **Bruin** — data pipeline (ingestr, Python, SQL assets)
- **Kafka** — event streaming
- **PostgreSQL 16** — databases
- **Airflow 2.9** — scheduling
- **Streamlit** — dashboards
- **Docker Compose** — orchestration

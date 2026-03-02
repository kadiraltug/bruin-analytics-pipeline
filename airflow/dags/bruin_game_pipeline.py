from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
}

BRUIN_EXEC = "docker exec bruin bruin run {asset}"


with DAG(
    dag_id="bruin_raw_ingest",
    start_date=datetime(2026, 1, 1),
    schedule_interval="* * * * *",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["bruin", "ingest"],
) as dag_raw:

    raw_game_events = BashOperator(
        task_id="raw_game_events",
        bash_command=BRUIN_EXEC.format(
            asset="bruin-pipeline/assets/ingest/game_events_clean.py"
        ),
    )


with DAG(
    dag_id="bruin_staging_and_marts",
    start_date=datetime(2026, 1, 1),
    schedule_interval="*/3 * * * *",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["bruin", "staging", "marts"],
) as dag_batch:

    stg_game_events = BashOperator(
        task_id="stg_game_events",
        bash_command=BRUIN_EXEC.format(
            asset="bruin-pipeline/assets/staging/stg_game_events.py"
        ),
    )

    daily_kpis = BashOperator(
        task_id="daily_kpis",
        bash_command=BRUIN_EXEC.format(
            asset="bruin-pipeline/assets/marts/daily_kpis.sql"
        ),
    )

    level_funnel_daily = BashOperator(
        task_id="level_funnel_daily",
        bash_command=BRUIN_EXEC.format(
            asset="bruin-pipeline/assets/marts/level_funnel_daily.sql"
        ),
    )

    churn_daily = BashOperator(
        task_id="churn_daily",
        bash_command=BRUIN_EXEC.format(
            asset="bruin-pipeline/assets/marts/churn_daily.sql"
        ),
    )

    stg_game_events >> [daily_kpis, level_funnel_daily, churn_daily]

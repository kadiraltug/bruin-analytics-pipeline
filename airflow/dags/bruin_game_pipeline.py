from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(seconds=20),
    "execution_timeout": timedelta(minutes=2),
}

BRUIN_EXEC = "docker exec bruin bruin run {asset}"


with DAG(
    dag_id="bruin_raw_ingest",
    start_date=datetime(2026, 1, 1),
    schedule_interval="* * * * *",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=3),
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
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=8),
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

    cohort_retention = BashOperator(
        task_id="cohort_retention",
        bash_command=BRUIN_EXEC.format(
            asset="bruin-pipeline/assets/marts/cohort_retention.sql"
        ),
    )

    churn_by_segment = BashOperator(
        task_id="churn_by_segment",
        bash_command=BRUIN_EXEC.format(
            asset="bruin-pipeline/assets/marts/churn_by_segment.sql"
        ),
    )

    time_to_churn = BashOperator(
        task_id="time_to_churn",
        bash_command=BRUIN_EXEC.format(
            asset="bruin-pipeline/assets/marts/time_to_churn.sql"
        ),
    )

    rolling_churn_7d = BashOperator(
        task_id="rolling_churn_7d",
        bash_command=BRUIN_EXEC.format(
            asset="bruin-pipeline/assets/marts/rolling_churn_7d.sql"
        ),
    )

    stg_game_events >> [
        daily_kpis,
        level_funnel_daily,
        churn_daily,
        cohort_retention,
        churn_by_segment,
        time_to_churn,
        rolling_churn_7d,
    ]

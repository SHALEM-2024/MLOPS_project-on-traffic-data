import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

IST = pendulum.timezone("Asia/Kolkata")

with DAG(
    dag_id="traffic_train_daily",
    description="Build features and train baseline daily",
    start_date=pendulum.now(IST).subtract(days=1),
    schedule="*/60 * * * *",  # 01:00 IST daily
    catchup=False,
    tags=["traffic", "train"],
) as dag:

    build = BashOperator(
        task_id="build_features",
        bash_command="cd /opt/airflow/project && python -m src.features.build",
        env={"PYTHONPATH": "/opt/airflow/project"},
    )

    train = BashOperator(
        task_id="train_logreg",
        bash_command="cd /opt/airflow/project && python -m src.models.train",
        env={"PYTHONPATH": "/opt/airflow/project"},
    )

    # dags/traffic_train_daily.py (add this task; keep build >> train)
    promote = BashOperator(
        task_id="promote_if_better",
        bash_command="cd /opt/airflow/project && python -m src.models.promote",
        env={"PYTHONPATH": "/opt/airflow/project", "MIN_IMPROVEMENT": "0.00"},
    )

    drift = BashOperator(
        task_id="drift_check",
        bash_command="cd /opt/airflow/project && python -m src.monitoring.drift",
        env={"PYTHONPATH": "/opt/airflow/project"},
    )

    build >> train >> promote >> drift



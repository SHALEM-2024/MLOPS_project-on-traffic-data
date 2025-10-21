# dags/fetch_5min.py
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

IST = pendulum.timezone("Asia/Kolkata")

with DAG(
    dag_id="traffic_fetch_5min",
    description="Fetch TomTom ETA every 5 minutes",
    start_date=pendulum.now(IST).subtract(days=1),
    schedule="*/2 * * * *",   # every 5 min
    catchup=False,
    tags=["traffic", "ingestion"],
) as dag:

    fetch = BashOperator(
        task_id="fetch_once",
        bash_command="cd /opt/airflow/project && python -m src.data.fetch",
        env={"PYTHONPATH": "/opt/airflow/project"},  # just to be explicit
    )

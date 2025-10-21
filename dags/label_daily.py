# dags/label_daily.py
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

IST = pendulum.timezone("Asia/Kolkata")

with DAG(
    dag_id="traffic_label_daily",
    description="Label and write Parquet once per day (IST midnight + 5 min)",
    start_date=pendulum.now(IST).subtract(days=1),
    schedule="*/10 * * * *",  # 00:05 IST daily
    catchup=False,
    tags=["traffic", "label"],
) as dag:

    label = BashOperator(
        task_id="clean_label_today",
        bash_command="cd /opt/airflow/project && python -m src.data.clean_label",
        env={"PYTHONPATH": "/opt/airflow/project"},
    )

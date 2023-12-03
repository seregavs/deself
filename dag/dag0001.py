from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime

default_args = {
    "start_date": datetime(2020, 1, 1),
    "owner": "airflow",
    "conn_id": "postgres_default"
}
 
with DAG(dag_id="dag0001", schedule_interval="@daily", default_args=default_args, catchup=False) as dag:
 
    begin = DummyOperator(task_id="begin")
    end = DummyOperator(task_id="end")
 
    begin >>  end
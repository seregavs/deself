from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
import pendulum

default_args_i028159 = {
    'owner': 'i028159',
    'email': ['sshablykin@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False
    # "conn_id": "postgres_default"
}
 
# Assuming:
# airflow.cfg::: default_timezone = utc
# my timezone is Europe/Moscow
# current time is 19:32
# the dag below runs at 19:40 very 10 minutes within 19 hrs
# Airflow executes the DAG after start_date + interval (daily)

with DAG( dag_id="basic_dag0001" \
         , start_date=pendulum.datetime(2023, 12, 2, tz="Europe/Moscow") \
         , schedule_interval="*/10 19 * * *" \
         , default_args=default_args_i028159 \
         , tags=["train", "basic"] \
        #  , owner_links={ "i028159": "mailto:sshablykin@gmail.com" } \ since version 2.4.0
         , catchup=False) as dag:
 
    begin = DummyOperator(task_id="begin")
    end = DummyOperator(task_id="end")
 
    begin >> end
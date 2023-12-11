from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pendulum
import sys
sys.path.append("/lessons/include")
# from ...include.etlxlsx001 import op_xlsx001
from etlxlsx001 import op_xlsx001

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

with DAG( dag_id="xlsxload_dag01" \
         , start_date=pendulum.datetime(2023, 12, 10, tz="Europe/Moscow") \
         , schedule_interval="*/40 19 * * *" \
         , default_args=default_args_i028159 \
         , tags=["train", "basic"] \
        #  , owner_links={ "i028159": "mailto:sshablykin@gmail.com" } \ since version 2.4.0
         , catchup=False) as dag:
 
    begin = DummyOperator(task_id="begin")
    end = DummyOperator(task_id="end")
 
    xlsx001 = PythonOperator(
        task_id='xlsx001',
        python_callable=op_xlsx001
    )

    begin >> xlsx001 >> end
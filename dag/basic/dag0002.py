from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
# from datetime import datetime
import pendulum
import logging

default_args_i028159 = {
    'owner': 'i028159',
    'email': ['sshablykin@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'is_paused_upon_creation': False  # Остановлен/запущен при появлении. Сразу запущен.
    # "conn_id": "postgres_default"
}

business_dt = '{{ ds }}' 

def step01():
    logging.info('INFO: step01')

def step02(ti):
    logging.info('INFO: step02')
    ti.xcom_push(key='key_step02', value='value_from_step02')

def step03(ti):
    logging.info('INFO: step03')
    key_step02 = ti.xcom_pull(key='key_step02')
    logging.info(f'INFO: key_step02 is {key_step02}')

def step04(bd, ti, **kwargs ):
    logging.info('INFO: step04')
    key_step02 = ti.xcom_pull(key='key_step02')
    logging.info(f'INFO: key_step02 is {key_step02}')
    logging.info(f'INFO: business_date is {bd}') 
    # https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html
    ts = str(kwargs['ts'])
    logging.info(f'INFO: business_ts is {ts}')     

with DAG(  dag_id="basic_dag0002" \
         , start_date=pendulum.datetime(2023, 12, 2, tz="Europe/Moscow") \
         , schedule_interval=None \
         , default_args=default_args_i028159 \
         , tags=["train", "basic"] \
        #  , owner_links={ "i028159": "mailto:sshablykin@gmail.com" } \ since version 2.4.0
         , catchup=False) as dag:
 
    begin = DummyOperator(task_id="begin")
    end = DummyOperator(task_id="end")

    step01 = PythonOperator(
        task_id='step01',
        python_callable=step01
    )
    step02 = PythonOperator(
        task_id='step02',
        python_callable=step02
    )
 
    step03 = PythonOperator(
        task_id='step03',
        python_callable=step03
    )

    step04 = PythonOperator(
        task_id='step04',
        python_callable=step04,
        op_kwargs={'bd': business_dt}
    )

    ( 
        begin >> step01 >> step02 >> step03 >> step04 >> end
    )
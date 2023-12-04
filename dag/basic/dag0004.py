from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
import pendulum
import logging
import sys
sys.path.append("/lessons/include")
# Иcпользовать такой путь для отладки. Перед публикацией - заменить на тот, что ниже
# from ...include.task001 import extract
# from ...include.task001 import transform
from task001 import extract
from task001 import transform

default_args_i028159 = {
    'owner': 'i028159',
    'email': ['sshablykin@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'is_paused_upon_creation': False  # Остановлен/запущен при появлении. Сразу запущен.
    # "conn_id": "postgres_default"
}

business_dt = '{{ ds }}' 

def branch01(ti):
    logging.info('branch01')
    ti.xcom_push(key='branch01', value='value_from_branch01')

def step03(ti):
    logging.info('step03')
    key_branch01 = ti.xcom_pull(key='branch01')
    logging.info(f'branch01 is {key_branch01}')

def step04(bd, ti, **kwargs ):
    logging.info('step04')
    key_branch01 = ti.xcom_pull(key='branch01')
    logging.info(f'key_step02 is {key_branch01}')
    logging.info(f'business_date is {bd}') 
    # https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html
    ts = str(kwargs['ts'])
    logging.info(f'business_ts is {ts}')


with DAG(  dag_id="basic_dag0004" \
         , start_date=pendulum.datetime(2023, 12, 2, tz="Europe/Moscow") \
         , schedule_interval=None \
         , default_args=default_args_i028159 \
         , tags=["train", "basic"] \
        #  https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/operators.html#jinja-templating
         , render_template_as_native_obj=True
         , catchup=False) as dag:
 
    begin   = DummyOperator(task_id="begin")
    # https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html#concepts-control-flow
    end     = DummyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)

    branch01 = PythonOperator(
        task_id='branch01',
        python_callable=branch01
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

    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform,
        op_kwargs={"order_data": "{{ti.xcom_pull('extract')}}"}
    )

    begin >> branch01
    branch01 >> step03 >> end
    branch01 >> step04 >> extract_task >> transform_task >> end
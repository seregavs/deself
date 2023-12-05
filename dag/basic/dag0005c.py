from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.filesystem import FileSensor
import pendulum

default_args_i028159 = {
    'owner': 'i028159',
    'email': ['sshablykin@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'is_paused_upon_creation': False  # Остановлен/запущен при появлении. Сразу запущен.
}

with DAG( dag_id='basic_dag0005c' \
         , start_date=pendulum.datetime(2023, 12, 4, tz="Europe/Moscow") \
         , schedule_interval="* * * * *" \
         , default_args=default_args_i028159 \
         , tags=["train", "basic"]  \
         , max_active_runs=1 \
         , render_template_as_native_obj=True) as dag :     
         
    start = DummyOperator(task_id='start')

# https://docs.astronomer.io/learn/what-is-a-sensor

# В соединении file system: fs_default настройка Extra {"path": "/deself_data/task02"}

    is_file_available = FileSensor(
        task_id='is_file_available', \
        filepath='', \
        # mode="reschedule", \
        mode="poke" \
        poke_interval=20,  # check every 20 seconds
    )

    end = DummyOperator(task_id='end')

    start >> is_file_available >> end
from airflow.providers.postgres.hooks.postgres import PostgresHook

# lvs = 'Sunday'
# print(lvs[:-2])

        postgres_hook = PostgresHook(postgres_conn_id)

import psycopg2
conn = psycopg2.connect("host=localhost dbname=postgres user=postgres")
cur = conn.cursor()
with open('user_accounts.csv', 'r') as f:
    # Notice that we don't need the csv module.
    next(f) # Skip the header row.
    cur.copy_from(f, 'users', sep=',')

conn.commit()

from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator

import datetime
import requests
import pandas as pd
import os
import psycopg2, psycopg2.extras

dag = DAG(
    dag_id='583_postgresql_mart_update',
    schedule_interval='0 0 * * *',
    start_date=datetime.datetime(2023, 5, 23),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=['example', 'example2'],
    params={"example_key": "example_value"},
)
business_dt = {'dt':'2022-05-06'}

###POSTGRESQL settings###
#set postgresql connectionfrom basehook
pg_conn = BaseHook.get_connection('pg_connection')

##init test connection
conn = psycopg2.connect(f"dbname='de' port='{pg_conn.port}' user='{pg_conn.login}' host='{pg_conn.host}' password='{pg_conn.password}'")
cur = conn.cursor()
cur.close()
conn.close()

#3. обновление таблиц d по загруженным данным в staging-слой

def update_mart_d_tables(ti):
    #connection to database
    psql_conn = BaseHook.get_connection('pg_connection')
    conn = psycopg2.connect(f"dbname='de' port='{psql_conn.port}' user='{psql_conn.login}' host='{psql_conn.host}' password='{psql_conn.password}'")
    cur = conn.cursor()

    cur.execute('delete from mart.f_daily_sales;')
    conn.commit()
    cur.execute('delete from mart.f_activity;')
    conn.commit()

    #d_calendar
    cur.execute('delete from mart.d_calendar')
    conn.commit()

    cur.execute(  "  insert into mart.d_calendar (" +
  "  select"+
  "      distinct to_char(date_id,'YYYYMMDD')::INTEGER as date_id, date_id::DATE as fact_date" +
  "          , EXTRACT(isodow from date_id) as day_num" +
  "          , extract(month from date_id) as month_num" +
  "          , to_char(date_id, 'Mon') as month_name" +
  "          , extract(year from date_id) as year_num " +
  "  from" +
  "      (" +
  "      select " +
  "          distinct date_id" +
  "      from" +
  "          stage.customer_research" +
  "  union all" +
  "      select" +
  "          distinct date_time" +
  "      from" +
  "          stage.user_activity_log" +
  "  union all" +
  "      select" +
  "          distinct date_time" +
  "      from" +
  "          stage.user_order_log) as s" +
  "  order by " +
  "      1 asc); ")
    conn.commit()


    #d_customer
    cur.execute('delete from mart.d_customer;')
    conn.commit()
    cur.execute(" insert into mart.d_customer (" +
" select customer_id, first_name, last_name, max(city_id) as city_id from stage.user_order_log" +
"  group by customer_id, first_name, last_name order by 1);")
    conn.commit()


    #d_item
    cur.execute('delete from mart.d_item;')
    conn.commit()

    cur.execute('insert into mart.d_item (select distinct item_id, item_name from stage.user_order_log order by 1);')
    conn.commit()

    cur.close()
    conn.close()

    return 200

#4. обновление витрин (таблицы f)
def update_mart_f_tables(ti):
    #connection to database
    psql_conn = BaseHook.get_connection('pg_connection')
    conn = psycopg2.connect(f"dbname='de' port='{psql_conn.port}' user='{psql_conn.login}' host='{psql_conn.host}' password='{psql_conn.password}'")
    cur = conn.cursor()

    #f_activity

    cur.execute(" insert into mart.f_activity (" +
" select action_id as activity_id, to_char(date_time,'YYYYMMDD')::INTEGER as date_id, sum(quantity) as click_number" +
"   from stage.user_activity_log" +
"   group by action_id, to_char(date_time,'YYYYMMDD')::INTEGER order by 1, 2);")
    conn.commit()

    #f_daily_sales


    cur.execute("insert into mart.f_daily_sales (" +
" select to_char(date_time,'YYYYMMDD')::INTEGER as date_id" +
"       , item_id" +
"       , customer_id" +
"       , avg(payment_amount / quantity) as price" +
"       , sum(quantity) as quantity" +
"       , sum(payment_amount) as payment_amount" +
"   from stage.user_order_log" +
"  group by to_char(date_time,'YYYYMMDD')::INTEGER, item_id, customer_id);")
    conn.commit()

    cur.close()
    conn.close()

    return 200


t_update_mart_d_tables = PythonOperator(task_id='update_mart_d_tables',
                                        python_callable=update_mart_d_tables,
                                        dag=dag)


t_update_mart_f_tables = PythonOperator(task_id='update_mart_f_tables',
                                        python_callable=update_mart_f_tables,
                                        dag=dag)


t_update_mart_d_tables >> t_update_mart_f_tables
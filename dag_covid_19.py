#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
import pandas as pd
import insert_data


with DAG(
    dag_id = 'dag_covid19',
    schedule_interval = '@daily',
    start_date = datetime(2022,4,26),
) as dag:
    insert_data_to_mysql = PythonOperator(
    task_id='insert_data_to_mysql',
    python_callable = insert_data.insert_data_to_mysql
    )


    insert_data_to_postgre = PythonOperator(
    task_id='insert_data_to_postgre',
    python_callable = insert_data.insert_data_to_postgre
    )

    insert_data_to_mysql >> insert_data_to_postgre


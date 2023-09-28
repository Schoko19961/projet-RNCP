from datetime import datetime, timedelta
from airflow.utils.db import create_session
from airflow.models import DAG, Variable
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.s3_list_operator import S3ListOperator
from typing import TYPE_CHECKING, Dict, List, Optional, Sequence, Union
import sys
from airflow.hooks.S3_hook import S3Hook
import subprocess

# Instantiate a DAG object
with DAG(
    dag_id="monitoring_yaml",
    schedule_interval='59 23 * * *', #daily at 23:59
    start_date=datetime(2023, 8, 31)
    #email =  ['maher.karboul@gmail.com'],
    #email_on_failure =  True,
    #mail_on_retry = False,
    #retries = 1,
    #retry_delay = timedelta(minutes=20),
) as dag:

    dependencies = BashOperator(
        task_id='install_dep',
         bash_command='python -m pip install -r /home/airflow/dags/requirements.txt',
     )

    ETL_pipeline = BashOperator(
        task_id='ETL_pipeline',
        bash_command='python /home/airflow/dags/main.py',
    )



# Set the order of execution of tasks. 
dependencies >> ETL_pipeline 
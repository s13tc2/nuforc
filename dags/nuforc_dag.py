import io
import json

from datetime import datetime, timedelta
import airflow.utils.dates
import pandas as pd
import requests
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import DAG
from requests.auth import HTTPBasicAuth

BUCKET_NAME = 'nuforc-bucket-8333'

dag = DAG(
    dag_id="nuforc_dag",
    schedule_interval="*/1 * * * *",
    start_date=datetime(2021, 1, 1),
    catchup=False,
)

def _fetch_nuforc_data_for_date(date_str=None, **context):
    nuforc_conn = BaseHook.get_connection(conn_id="nuforc")

    if date_str is None:

        date_str = context["dag"].start_date.strftime("%Y-%m-%d")

    url = f"http://{nuforc_conn.host}:{nuforc_conn.port}/sightings/{date_str}"
    response = requests.get(
        url, auth=HTTPBasicAuth(nuforc_conn.login, nuforc_conn.password)
    )
    data = response.json()
    print(data)

    s3_hook = S3Hook(aws_conn_id="my_aws_conn")

    s3_hook.load_string(
        string_data=json.dumps(data),
        key=f"raw/nuforc/{date_str}.json",
        bucket_name=BUCKET_NAME,
    )

def _fetch_nuforc_data_for_next_day(**context):
    next_date = (context["dag"].start_date + timedelta(days=1)).strftime("%Y-%m-%d")
    _fetch_nuforc_data_for_date(next_date)
   
fetch_nuforc_data_for_date = PythonOperator(
    task_id="fetch_nuforc_data_for_date", python_callable=_fetch_nuforc_data_for_date, dag=dag
)

fetch_nuforc_data_task = PythonOperator(
    task_id="fetch_nuforc_data_for_next_day",
    python_callable=_fetch_nuforc_data_for_next_day,
    provide_context=True,
    dag=dag,
)

fetch_nuforc_data_task

import io
import json

from datetime import datetime, timedelta
import airflow.utils.dates
import pandas as pd
import requests
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import DAG
from requests.auth import HTTPBasicAuth
from nuforc.operators.pandas_operator import PandasOperator
from nuforc.operators.s3_to_postgres import S3PandasToPostgres

BUCKET_NAME = Variable.get("BUCKET")

dag = DAG(
    dag_id="nuforc_dag",
    schedule_interval="*/1 * * * *",
    start_date=datetime(2021, 1, 1),
    catchup=False,
)

def _fetch_nuforc_data_for_date(**context):
    nuforc_conn = BaseHook.get_connection(conn_id="nuforc")
    
    last_fetched_date_str = Variable.get("last_fetched_date", default_var=datetime(2021, 1, 1).strftime("%Y-%m-%d"))
    last_fetched_date = datetime.strptime(last_fetched_date_str, "%Y-%m-%d")

    Variable.set("current_fetched_data", last_fetched_date_str)

    date_to_fetch = last_fetched_date + timedelta(days=1)
    date_str = date_to_fetch.strftime("%Y-%m-%d")
    
    url = f"http://{nuforc_conn.host}:{nuforc_conn.port}/sightings/{date_str}"
    response = requests.get(url, auth=HTTPBasicAuth(nuforc_conn.login, nuforc_conn.password))
    data = response.json()

    s3_hook = S3Hook(aws_conn_id="my_aws_conn")

    s3_hook.load_string(
        string_data=json.dumps(data),
        key=f"raw/nuforc/{date_str}.json",
        bucket_name=BUCKET_NAME,
    )

    Variable.set("last_fetched_date", date_to_fetch.strftime("%Y-%m-%d"))

fetch_nuforc_data_for_date = PythonOperator(
    task_id="fetch_nuforc_data_for_date",
    python_callable=_fetch_nuforc_data_for_date,
    dag=dag
)

def get_s3_object(
    pandas_read_callable, bucket, paths, pandas_read_callable_kwargs=None
):
    """
    Fetches data from Amazon S3 bucket and returns a concatenated DataFrame.

    Args:
        pandas_read_callable (callable): Pandas read function to read data from S3.
        bucket (str): S3 bucket name.
        paths (str or list): S3 object path or list of S3 object paths.
        pandas_read_callable_kwargs (dict, optional): Additional keyword arguments to pass to the
            pandas read function. Defaults to None.

    Returns:
        pd.DataFrame: Concatenated DataFrame from the fetched data.
    """
    if isinstance(paths, str):
        if paths.startswith("[") and paths.endswith("]"):
            paths = eval(paths)
        else:
            paths = [paths]

    if pandas_read_callable_kwargs is None:
        pandas_read_callable_kwargs = {}

    dfs = []
    s3_hook = S3Hook(aws_conn_id="my_aws_conn") # Update with your AWS connection ID
    for path in paths:
        s3_object = s3_hook.get_key(key=path, bucket_name=bucket)
        df = pandas_read_callable(s3_object.get()['Body'], **pandas_read_callable_kwargs)
        dfs.append(df)
    return pd.concat(dfs)


def transform_nuforc_data(df):
    pass


def write_s3_object(
    df, pandas_write_callable, bucket, path, pandas_write_callable_kwargs=None
):
    """
    Writes a DataFrame to Amazon S3 bucket using the specified pandas write function.

    Args:
        df (pd.DataFrame): DataFrame to be written to S3.
        pandas_write_callable (callable): Pandas write function to write DataFrame to S3.
        bucket (str): S3 bucket name.
        path (str): S3 object path.
        pandas_write_callable_kwargs (dict, optional): Additional keyword arguments to pass to the
            pandas write function. Defaults to None.
    """
    if pandas_write_callable_kwargs is None:
        pandas_write_callable_kwargs = {}

    s3_hook = S3Hook(aws_conn_id="my_aws_conn") # Update with your AWS connection ID

    bytes_buffer = io.BytesIO()
    pandas_write_method = getattr(df, pandas_write_callable.__name__)
    pandas_write_method(bytes_buffer, **pandas_write_callable_kwargs)
    bytes_buffer.seek(0)
    s3_hook.load_bytes(
        bytes_buffer.getvalue(),
        key=path,
        bucket_name=bucket,
        replace=True
    )


process_nuforc_data = PandasOperator(
    task_id="process_nuforc_data",
    input_callable=get_s3_object,
    input_callable_kwargs={
        "pandas_read_callable": pd.read_json,
        "bucket": BUCKET_NAME,
        "paths": "raw/nuforc/{{ var.value.current_fetched_data }}.json",
    },
    transform_callable=None,
    output_callable=write_s3_object,
    output_callable_kwargs={
        "bucket": BUCKET_NAME,
        "path": "processed/nuforc/{{ var.value.current_fetched_data }}.parquet",
        "pandas_write_callable": pd.DataFrame.to_parquet,
        "pandas_write_callable_kwargs": {"engine": "auto"},
    },
    dag=dag,
)

fetch_nuforc_data_for_date >> process_nuforc_data

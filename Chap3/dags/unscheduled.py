import datetime as dt 
import pandas as pd

from pathlib import Path 
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

dag = DAG(
    dag_id              = "01_unscheduled",
    start_date          = dt.datetime(2019, 1, 1),
    schedule_interval   = None,
)

fetch_events = BashOperator(
    task_id             = "fetch_events",
    bash_command        = (
        "mkdir -p /data && "
        "curl -o /data/events.json "
        "http://localhost:5000/events"
    ),
    dag = dag
)

def _calculate_stats(input_path, output_path):
    """Calculates event statistics."""
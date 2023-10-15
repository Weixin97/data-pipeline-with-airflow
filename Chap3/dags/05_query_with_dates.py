
import datetime as dt 
import pandas as pd

from pathlib import Path 
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

dag = DAG(
    dag_id              = "01_unscheduled",
    start_date          = dt.datetime(2023, 10, 14),
    end_date            = dt.datetime(2023, 10, 15),
    schedule_interval   = dt.timedelta(days=3), # run DAG every three days
)

fetch_events = BashOperator(
    task_id             = "fetch_events",
    bash_command        = (
        "mkdir -p /data && "
        "curl -o /data/events.json "
        "http://localhost:5000/events?"
        "start_date=2019-01-01&"
        "end_date=2019-01-02"
    ),
    dag = dag
)

def _calculate_stats(input_path, output_path):
    """Calculates event statistics."""
    Path(output_path).parent.mkdir(exist_ok=True)

    events              = pd.read_json(input_path)
    stats               = events.groupby(["date", "user"])\
                                .size().reset_index()
    stats.to_csv(output_path, index = False)

calculate_stats = PythonOperator(
    task_id             = "calculate_stats",
    python_callable     = _calculate_stats,
    op_kwargs           = {"input_path" : "/data/events.json",
                           "output_path": "/data/stats.csv"},
    dag = dag,
)

fetch_events >> calculate_stats
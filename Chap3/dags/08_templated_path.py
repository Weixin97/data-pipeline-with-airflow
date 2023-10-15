
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
        "curl -o /data/events/{{ds}}.json " # partition file name
        "http://localhost:5000/events?"
        "start_date={{ds}}" # using ds short hand notation YYYY-MM-DD
        "&end_date={{next_ds}}" # next_execution_date holds the execution date of the next interval
    ),
    dag = dag
)

def _calculate_stats(**context): # take the dict input
    """Calculates event statistics."""
    input_path          = context["templates_dict"]["input_path"] #(2) Retrieve the templated values from the templates_dict object
    output_path         = context["templates_dict"]["output_path"]
    Path(output_path).parent.mkdir(exit_ok=True)

    events              = pd.read_json(input_path)
    stats               = events.groupby(["date", "user"])\
                                .size().reset_index()
    stats.to_csv(output_path, index = False)

calculate_stats = PythonOperator(
    task_id             = "calculate_stats",
    python_callable     = _calculate_stats,
    templates_dict      = {
        "input_path" : "/data/events/{{ds}}.json", #(3) Pass the values that we want to be templated.
        "output_path": "/data/stats/{{ds}}.csv"},
    dag = dag,
)

fetch_events >> calculate_stats
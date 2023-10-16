import airflow.utils.dates 
from airflow import DAG 
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

dag = DAG(
    dag_id              = "chapter4_stocksense_bashoperator",
    start_date          = airflow.utils.dates.days_ago(3),
    schedule_interval   = "@hourly",
)

get_data = BashOperator(
    task_id             = "get_data",
    bash_command        = (
        "curl -o /tmp/wikipageviews.gz "
        "https://dumps.wikimedia.org/other/pageviews/"
        "{{ execution_date.year }}/"
        "{{ execution_date.year}}-"
        "{{ '{:0.2}'.format(execution_date.month)}}/"
        "pageviews-{{ execution_date.year }}"
        "{{ '{{:0.2}}'.format(execution_date.month) }}"
        "{{ '{{:0.2}}'.format(execution_date.day) }}-"
        "{{ '{{:0.2}}'.format(execution_date.hour) }}0000.gz"
    ),
    dag = dag,
)

def _print_context(**kwargs):
    print(kwargs)

print_context = PythonOperator(

    """
    Running this task print a dict of all available variables in the task context.
    """
    task_id             = "print_context",
    python_callable     = _print_context,
    dag = dag,
)



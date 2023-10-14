import json
import pathlib 

import airflow.utils.dates 
import airflow
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG 
from airflow.operators.bash_operator import BashOperator 
from airflow.operators.python_operator import PythonOperator


"""
DAG class takes 2 required arguments.
1. The name of the DAG displayed in the Airflow user interface (UI)
2. The datetime at which the workflow should first start running
"""
dag = DAG( #(1) Instantiate a DAG object; this is the starting point of any workflow
    dag_id              = "download_rocket_launches", #(2) The name of the DAG
    start_date          = airflow.utils.dates.days_ago(14), #(3) The date at which the DAG should first start running.
    schedule_interval   = None, #(4) At what interval the DAG should run, eg. "@daily"
)

"""
Bash Operator take 3 arguments:
1. The name of the task
2. The Bash command to execute
3. Reference to the DAG variable
"""
download_launches = BashOperator( # (5) Apply Bash to download the URL response with curl
    task_id            = "download_launches", # (6) The name of the task
    bash_command       = "curl -Lk -o /tmp/launches.json 'https://ll.thespacedevs.com/2.0.0/launch/upcoming'",
    dag                = dag,
)

def _get_pictures(): #(7) A Python function will parse the response and download all rocket pictures
    # Ensure directory exists
    pathlib.Path("/tmp/images").mkdir(parents=True, exist_ok=True)

    # Download all pics in launches.json
    with open("/tmp/launches.json") as f:
        launches    = json.load(f)
        image_urls  = [launch["image"] for launch in launches["results"]]
        for image_url in image_urls:
            try:
                response        = requests.get(image_url)
                image_filename  = image_url.split("/")[-1]
                target_file     = f"/tmp/images/{image_filename}"
                with open(target_file, "wb") as f:
                    f.write(response.content)
                print(f"Download {image_url} to {target_file}")

            except requests_exceptions.MissingSchema:
                print(f"{image_url} appears to be an invalid URL.")
            except requests_exceptions.ConnectionError:
                print(f"Could not connect to {image_url}")

"""
Python Operator
1. Instantiate a PythonOperator to call the Python function.
2. Point to the Python function to execute.
"""
get_pictures = PythonOperator( #(8) Call the Python function in the DAG with a PythonOperator
    task_id         = "get_pictures",
    python_callable = _get_pictures, #(8)
    dag             = dag,
)

notify = BashOperator(
    task_id         = "notify",
    bash_command    = 'echo "There are now $(ls /tmp/images/ | wx -l) images."',
    dag             = dag,
)

# >> : binary right shift operator
download_launches >> get_pictures >> notify #(9) Set the order of execution of tasks
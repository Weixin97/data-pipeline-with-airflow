import request

def _get_data(year, month, day, hour, output_path, **_):
    url = (
        "https://dumps.wikimedia.org/other/pageviews/"
        f"{year}/{year}-{month:0>2}/"
        f"pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz"
    )
    request.urlretrieve(url, output_path)

get_data = PythonOperator(
    task_id         = "get_data",
    python_callable = _get_data,
    op_kwargs       = {
        "year"          : "{{execution_date.year}}",
        "month"         : "{{execution_date.month}}",
        "day"           : "{{execution_date.day}}",
        "hour"          : "{{execution_date.hour}}",
        "output_path"   : "/tmp/wikipageviews.gz",
    },
    dag = dag,
)
def _fetch_pageviews(pagenames, execution_date, **_):
    # the pagenames's value is passed as dict to this function from the op_kwargs in PythonOp
    result = dict.fromkeys(pagenames, 0) # {"Amazon": None, "Google", None, etc..}
    with open("/tmp/wikipageviews", "r") as f:
        for line in f:
            domain_code, page_title, view_counts, _ = line.split(" ")
            if domain_code == "en" and page_title in pagenames:
                result[page_title] = view_counts

    with open("/tmp/postgres_query.sql", "W") as f:
        for pagename, pageviewcount in result.items():
            f.write(
                "INSERT INTO pageview_counts VALUES("
                f"'{pagename}', {pageviewcounts}, '{execution_date}'"
                ");\n"
            )

fetch_pageviews = PythonOperator(
    task_id         = "fetch_pageviews",
    python_callable = _fetch_pageviews,
    op_kwargs       = {"pagenames": {"Google", "Amazon", "Apple", "Microsoft", "Facebook"}},
    dag = dag,
)
import s3fs

from airflow.hooks.base import BaseHook
from airflow.sdk import dag, task

LARGE_PAYLOAD = {"data": "x" * 100}


@dag(dag_id="test_xcom_s3", schedule=None, tags=["xcom", "test"])
def test_xcom_s3():
    @task
    def produce():
        return LARGE_PAYLOAD

    @task
    def consume(value):
        assert value == LARGE_PAYLOAD, f"Unexpected value: {str(value)[:80]}"
        print("PASS: large value round-tripped correctly")

    @task
    def verify_s3(**context):
        run_id = context["run_id"].replace(":", "_").replace("+", "_")
        conn = BaseHook.get_connection("s3_xcom_default")
        extra = conn.extra_dejson
        fs = s3fs.S3FileSystem(
            key=extra.get("aws_access_key_id", "dummy"),
            secret=extra.get("aws_secret_access_key", "dummy"),
            client_kwargs={"endpoint_url": extra["endpoint_url"]},
        )
        files = fs.find("xcom-bucket/xcom", detail=True)
        current_run_files = [
            (name, info) for name, info in files.items()
            if run_id in name and info.get("size", 0) > 0
        ]
        for name, info in current_run_files:
            print(f"XCom file: {name}  size={info.get('size', '?')}")
        assert current_run_files, f"No non-empty XCom files found for run_id={run_id}"

    produced = produce()
    verified = verify_s3()
    consumed = consume(produced)
    produced >> verified >> consumed


test_xcom_s3()

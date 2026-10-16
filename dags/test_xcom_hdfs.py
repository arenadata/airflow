from fsspec.implementations.webhdfs import WebHDFS

from airflow.hooks.base import BaseHook
from airflow.sdk import dag, task

LARGE_PAYLOAD = {"data": "x" * 100}


@dag(dag_id="test_xcom_hdfs", schedule=None, tags=["xcom", "test"])
def test_xcom_hdfs():
    @task
    def produce():
        return LARGE_PAYLOAD

    @task
    def consume(value):
        assert value == LARGE_PAYLOAD, f"Unexpected value: {str(value)[:80]}"
        print("PASS: large value round-tripped correctly")

    @task
    def verify_hdfs(**context):

        run_id = context["run_id"].replace(":", "_").replace("+", "_")
        conn = BaseHook.get_connection("hdfs_webhdfs_default")
        fs = WebHDFS(host=conn.host, port=conn.port, user=conn.login or None)
        files = fs.find("/tmp/xcom", detail=True)
        current_run_files = [
            (name, info) for name, info in files.items()
            if run_id in name and info.get("size", 0) > 0
        ]
        for name, info in current_run_files:
            print(f"XCom file: {name}  size={info.get('size', '?')}")
        assert current_run_files, f"No non-empty XCom files found for run_id={run_id}"

    produced = produce()
    verified = verify_hdfs()
    consumed = consume(produced)
    produced >> verified >> consumed


test_xcom_hdfs()

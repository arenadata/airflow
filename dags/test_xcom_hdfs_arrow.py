from airflow.sdk import dag, task
import pyarrow.fs as pafs
from airflow.hooks.base import BaseHook

LARGE_PAYLOAD = {"id": [1, 2, 3], "value": ["foo", "bar", "baz"]}


@dag(dag_id="test_xcom_hdfs_arrow", schedule=None, tags=["xcom", "hdfs", "arrow"])
def test_xcom_hdfs_arrow():

    @task
    def produce() -> dict:
        return LARGE_PAYLOAD

    @task
    def verify_hdfs(**context):
        conn = BaseHook.get_connection("hdfsarrow_default")
        fs = pafs.HadoopFileSystem(conn.host, port=conn.port or 8020)
        run_id = context["run_id"].replace(":", "_").replace("+", "_")
        selector = pafs.FileSelector("/xcom", recursive=True)
        files = [
            f for f in fs.get_file_info(selector)
            if f.type == pafs.FileType.File and run_id in f.path and f.size > 0
        ]
        for f in files:
            print(f"XCom file: {f.path}  size={f.size}")
        assert files, f"No XCom files found in HDFS for run_id={run_id}"

    @task
    def consume(data: dict):
        assert data == LARGE_PAYLOAD, f"Unexpected: {data}"
        print("PASS: XCom round-trip over HDFS Arrow backend succeeded")

    produced = produce()
    verified = verify_hdfs()
    consumed = consume(produced)
    produced >> verified >> consumed


test_xcom_hdfs_arrow()

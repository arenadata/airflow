from airflow.sdk import dag, task
import pyarrow.fs as pafs
from airflow.hooks.base import BaseHook
from airflow.sdk.execution_time.xcom import XCom
from airflow.hooks.base import BaseHook

LARGE_PAYLOAD = {"id": [1, 2, 3], "value": ["foo", "bar", "baz"]}


@dag(dag_id="test_xcom_hdfs_purge", schedule=None, tags=["xcom", "hdfs", "purge"])
def test_xcom_hdfs_purge():

    @task
    def produce() -> dict:
        return LARGE_PAYLOAD

    @task
    def verify_file_exists(**context):
        conn = BaseHook.get_connection("hdfsarrow_default")
        fs = pafs.HadoopFileSystem(conn.host, port=conn.port or 8020)
        run_id = context["run_id"].replace(":", "_").replace("+", "_")
        selector = pafs.FileSelector("/xcom", recursive=True)
        files = [
            f for f in fs.get_file_info(selector)
            if f.type == pafs.FileType.File and run_id in f.path and f.size > 0
        ]
        assert files, f"No XCom files found in HDFS for run_id={run_id}"
        for f in files:
            print(f"EXISTS: {f.path}  size={f.size}")

    @task
    def delete_xcom(**context):
        run_id = context["run_id"]
        dag_id = context["dag_run"].dag_id
        for key in ("return_value", "id", "value"):
            XCom.delete(key=key, task_id="produce", dag_id=dag_id, run_id=run_id)
            print(f"Deleted XCom key={key}")

    @task
    def verify_file_deleted(**context):
        conn = BaseHook.get_connection("hdfsarrow_default")
        fs = pafs.HadoopFileSystem(conn.host, port=conn.port or 8020)
        run_id = context["run_id"].replace(":", "_").replace("+", "_")
        selector = pafs.FileSelector("/xcom", recursive=True)
        files = [
            f for f in fs.get_file_info(selector)
            if f.type == pafs.FileType.File and run_id in f.path and f.size > 0
        ]
        assert not files, f"XCom files still exist in HDFS after delete: {[f.path for f in files]}"
        print("PASS: XCom files deleted from HDFS after XCom.delete()")

    produced = produce()
    verified = verify_file_exists()
    deleted = delete_xcom()
    verified_gone = verify_file_deleted()

    produced >> verified >> deleted >> verified_gone


test_xcom_hdfs_purge()

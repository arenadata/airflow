from airflow.sdk import dag, task

LARGE_PAYLOAD = {"data": "x" * 100}


@dag(dag_id="test_xcom_webhdfs", schedule=None, tags=["xcom", "test"])
def test_xcom_webhdfs():
    @task
    def produce():
        return LARGE_PAYLOAD

    @task
    def consume(value):
        assert value == LARGE_PAYLOAD, f"Unexpected value: {str(value)[:80]}"
        print("PASS: large value round-tripped correctly")

    @task
    def verify_ozone(**context):
        import time
        time.sleep(5)
        from fsspec.implementations.webhdfs import WebHDFS
        from airflow.hooks.base import BaseHook
        run_id = context["run_id"].replace(":", "_").replace("+", "_")
        conn = BaseHook.get_connection("ozone_webhdfs_default")
        fs = WebHDFS(host=conn.host, port=conn.port, user=conn.login or None)
        files = fs.find("/vol1/bucket-legacy", detail=True)
        print(f"All files in /vol1/bucket-legacy: {len(files)}")
        for name, info in files.items():
            print(f"  {name}  size={info.get('size', '?')}")
        current_run_files = [
            (name, info) for name, info in files.items()
            if run_id in name and info.get("size", 0) > 0
        ]
        assert current_run_files, f"No non-empty XCom files found for run_id={run_id}"

    produced = produce()
    verified = verify_ozone()
    consumed = consume(produced)
    produced >> verified >> consumed


test_xcom_webhdfs()

from airflow.sdk import dag, task
import pyarrow.fs as pafs

LARGE_PAYLOAD = {"id": [1, 2, 3], "value": ["foo", "bar", "baz"], "extra": "y" * 100}


@dag(dag_id="test_xcom_ofs", schedule=None, tags=["ofs", "xcom"])
def test_pyarrow_ofs():

    @task
    def produce() -> dict:
        return LARGE_PAYLOAD

    @task
    def verify_ofs(**context):
        run_id = context["run_id"].replace(":", "_").replace("+", "_")
        fs = pafs.HadoopFileSystem("default")
        selector = pafs.FileSelector("s3v/ofs-bucket/xcom", recursive=True)
        files = fs.get_file_info(selector)
        current_run_files = [
            f for f in files
            if run_id in f.path and (f.size or 0) > 0
        ]
        for f in current_run_files:
            print(f"XCom file: {f.path}  size={f.size}")
        assert current_run_files, f"No XCom files found in OFS for run_id={run_id}"

    @task
    def consume(data: dict):
        assert data == LARGE_PAYLOAD, f"Unexpected: {data}"
        print("PASS: XCom round-trip over OFS backend succeeded")

    produced = produce()
    verified = verify_ofs()
    consumed = consume(produced)
    produced >> verified >> consumed


test_pyarrow_ofs()

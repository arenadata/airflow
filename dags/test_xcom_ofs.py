from airflow.sdk import dag, task
import fsspec

LARGE_PAYLOAD = {"id": [1, 2, 3], "value": ["foo", "bar", "baz"], "extra": "y" * 100}


@dag(dag_id="test_xcom_ofs", schedule=None, tags=["ofs", "xcom"])
def test_pyarrow_ofs():

    @task
    def produce() -> dict:
        return LARGE_PAYLOAD

    @task
    def verify_ofs(**context):
        run_id = context["run_id"].replace(":", "_").replace("+", "_")
        fs = fsspec.filesystem("ozonearrow")
        files = fs.find("s3v/ofs-bucket/xcom", detail=True)
        current_run_files = [
            (name, info) for name, info in files.items()
            if run_id in name and info.get("size", 0) > 0
        ]
        for name, info in current_run_files:
            print(f"XCom file: {name}  size={info['size']}")
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

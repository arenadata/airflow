from airflow.sdk import dag, task

LARGE_PAYLOAD = {"id": [1, 2, 3], "value": ["foo", "bar", "baz"]}


@dag(dag_id="test_xcom_ofs", schedule=None, tags=["ofs", "xcom"])
def test_pyarrow_ofs():

    @task
    def produce() -> dict:
        return LARGE_PAYLOAD

    @task
    def consume(data: dict):
        assert data == LARGE_PAYLOAD, f"Unexpected: {data}"
        print("PASS: XCom round-trip over OFS backend succeeded")

    consume(produce())


test_pyarrow_ofs()

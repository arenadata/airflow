import os

os.environ.setdefault("AIRFLOW__COMMON_IO__XCOM_OBJECTSTORAGE_PATH", "file:///tmp/airflow-xcom")
os.environ.setdefault("AIRFLOW__COMMON_IO__XCOM_OBJECTSTORAGE_THRESHOLD", "0")

from airflow.sdk import dag, task

LARGE_PAYLOAD = {"data": "x" * 10_000}


@dag(dag_id="test_xcom_offload_explicit", schedule=None, tags=["xcom", "test"])
def test_xcom_offload_explicit():
    @task
    def produce(**context):
        from airflow.providers.common.io.xcom.backend import _get_threshold, _get_base_path
        _get_threshold.cache_clear()
        _get_base_path.cache_clear()
        ti = context["ti"]
        ti.xcom_push(key="my_data", value=LARGE_PAYLOAD)
        print(f"Pushed to XCom: key=my_data, threshold={_get_threshold()}, base_path={_get_base_path()}")

    @task
    def consume(**context):
        from airflow.providers.common.io.xcom.backend import _get_threshold, _get_base_path
        _get_base_path.cache_clear()
        _get_threshold.cache_clear()
        ti = context["ti"]
        value = ti.xcom_pull(task_ids="produce", key="my_data")
        assert value == LARGE_PAYLOAD, f"Unexpected value: {str(value)[:80]}"
        print("PASS: large value round-tripped correctly via explicit xcom_push/pull")

    produce() >> consume()


test_xcom_offload_explicit()

import os

# The DAG file is parsed by the scheduler, which may not have xcom_objectstorage_path
# and xcom_objectstorage_threshold in its config. _get_base_path() and _get_threshold()
# are decorated with @cache and fill the cache on first call, at parse time
# Setting env vars here ensures the cache is filled with correct values when the
# scheduler imports this file. setdefault preserves any values already set in the environment
os.environ.setdefault("AIRFLOW_CONFIG", "/etc/airflow/conf/airflow.cfg")
os.environ.setdefault("AIRFLOW__COMMON_IO__XCOM_OBJECTSTORAGE_PATH", "file:///tmp/airflow-xcom")
os.environ.setdefault("AIRFLOW__COMMON_IO__XCOM_OBJECTSTORAGE_THRESHOLD", "0")

# _get_base_path() and _get_threshold() are decorated with @cache and are populated
# on first call. The DAG file is parsed by the scheduler (under a different user/env),
# which causes the cache to be filled with wrong defaults before the task process starts
# We clear the cache here so it is reread from the env vars set above
from airflow.providers.common.io.xcom.backend import _get_threshold, _get_base_path
_get_threshold.cache_clear()
_get_base_path.cache_clear()
print(f"AT IMPORT: threshold={_get_threshold()}, base_path={_get_base_path()}")

from airflow.sdk import dag, task


LARGE_PAYLOAD = {"data": "x" * 10_000}


@dag(dag_id="test_xcom_offload", schedule=None, tags=["xcom", "test"])
def test_xcom_offload():
    @task
    def produce():
        from airflow.providers.common.io.xcom.backend import _get_threshold, _get_base_path
        _get_threshold.cache_clear()
        _get_base_path.cache_clear()
        return LARGE_PAYLOAD

    @task
    def consume(value):
        from airflow.providers.common.io.xcom.backend import _get_threshold, _get_base_path
        _get_threshold.cache_clear()
        _get_base_path.cache_clear()
        assert value == LARGE_PAYLOAD, f"Unexpected value: {str(value)[:80]}"
        print("PASS: large value round-tripped correctly")

    consume(produce())


test_xcom_offload()

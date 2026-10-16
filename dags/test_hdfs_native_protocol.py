from airflow.sdk import dag, task


@dag(dag_id="test_hdfs_native_protocol", schedule=None, tags=["hdfs-native", "research"])
def test_hdfs_native_protocol():

    @task
    def connect():
        from hdfs_native import Client
        client = Client("hdfs://ozone:9862")
        # любой вызов инициирует RPC handshake
        list(client.list_status("/"))

    connect()


test_hdfs_native_protocol()

from airflow.sdk import dag, task


@dag(dag_id="test_snakebite_protocol", schedule=None, tags=["snakebite", "research"])
def test_snakebite_protocol():

    @task
    def connect():
        from snakebite.client import Client
        client = Client("ozone", 9862)
        # любой вызов инициирует RPC handshake
        list(client.ls(["/"])) 

    connect()


test_snakebite_protocol()

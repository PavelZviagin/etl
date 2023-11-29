import backoff
import elastic_transport
from elasticsearch import Elasticsearch, helpers


class ElasticClient:

    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self.client = None

    @backoff.on_exception(backoff.expo, Exception, max_time=60)
    def __enter__(self):
        self.client: Elasticsearch = Elasticsearch(self.connection_string)
        if not self.client.ping():
            raise Exception("Elasticsearch is not available")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.client.close()

    @backoff.on_exception(backoff.expo, elastic_transport.ConnectionError)
    def create_index(self, index_name: str, index_body: dict):
        if not self.client.indices.exists(index=index_name):
            self.client.indices.create(index=index_name, body=index_body)

    @backoff.on_exception(backoff.expo, elastic_transport.ConnectionError)
    def load_data(self, data: list[dict]) -> None:
        helpers.bulk(self.client, data)

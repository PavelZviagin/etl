import json
import time

from loguru import logger

from connectors.elasticsearch import ElasticClient
from connectors.postgres import PostgresClient
from transformers.transform import TransformFactory
from settings import settings
from static.sql import MOVIES_SQL, GENRES_SQL, PERSONS_SQL
from storage.state import State
from storage.storages import JsonFileStorage

schemas = [
    {
        'index_name': 'movies',
        'index_body_path': 'static/index_film_work.json',
        'sql_query': MOVIES_SQL
    },
    {
        'index_name': 'genres',
        'index_body_path': 'static/index_genres.json',
        'sql_query': GENRES_SQL
    },
    {
        'index_name': 'persons',
        'index_body_path': 'static/index_persons.json',
        'sql_query': PERSONS_SQL
    }
]


def etl(pg_client: PostgresClient, elastic_client: ElasticClient, schema: dict) -> None:
    for data in pg_client.get_data(schema):
        transformer = TransformFactory.get_transformer(schema['index_name'])
        transformed_data = transformer.transform(data)
        elastic_client.load_data(transformed_data)
        logger.info(f"Loaded {len(transformed_data)} rows to index {schema['index_name']}")


def load_json(index_body_path):
    with open(index_body_path, 'r') as f:
        return json.load(f)


def load_data(state):
    with (ElasticClient(settings.elastic_connection_string) as elastic_client, PostgresClient(settings.dsl, state) as pg_client):
        for schema in schemas:
            elastic_client.create_index(
                schema['index_name'], load_json(schema['index_body_path'])
            )

            etl(pg_client, elastic_client, schema)


if __name__ == "__main__":
    state = State(storage=JsonFileStorage("static/state.json"))

    while True:
        load_data(state)
        logger.info(f"Sleeping for {settings.delay} seconds")
        time.sleep(settings.delay)

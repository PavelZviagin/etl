from datetime import datetime

import backoff
import psycopg2
from psycopg2.extras import DictCursor

from settings import settings
from storage.state import State


class PostgresClient:
    def __init__(self, dsl: dict, state: State):
        self.dsl = dsl
        self.state = state
        self.pg_conn = None

    @backoff.on_exception(backoff.expo, psycopg2.OperationalError, max_tries=5)
    def __enter__(self):
        self.pg_conn = psycopg2.connect(**self.dsl, cursor_factory=DictCursor)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.pg_conn.close()

    def _get_last_date(self, prefix: str):
        curr_date = self.state.get_state(f"last_date_{prefix}")
        if not curr_date:
            curr_date = datetime.min.isoformat()

        return curr_date

    def _save_last_date_from_row(self, row: dict, prefix: str) -> None:
        self.state.set_state(f"last_date_{prefix}", row["updated_at"].isoformat())

    @backoff.on_exception(backoff.expo, psycopg2.OperationalError, max_tries=5)
    def get_data(self, schema) -> list[dict]:
        sql: str = schema['sql_query']
        prefix = schema['index_name']

        counter = sql.count("%s")

        curr_date = (self._get_last_date(prefix),) * counter

        with self.pg_conn.cursor() as curs:
            curs.execute(
                sql,
                curr_date,
            )

            while rows := curs.fetchmany(settings.batch_size):
                yield rows
                self._save_last_date_from_row(rows[-1], prefix)

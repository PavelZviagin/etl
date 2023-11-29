import json
import os

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    elastic_connection_string: str

    postgres_db: str
    postgres_user: str
    postgres_password: str
    postgres_host: str
    postgres_port: int

    batch_size: int = 1000
    delay: int = 60

    @property
    def dsl(self):
        return {
            "dbname": self.postgres_db,
            "user": self.postgres_user,
            "password": self.postgres_password,
            "host": self.postgres_host,
            "port": self.postgres_port,
        }


    class Config:
        env_file = ".env"


settings = Settings()

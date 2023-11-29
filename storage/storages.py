import abc
import json
import os
from typing import Any, Dict


class BaseStorage(abc.ABC):
    """Абстрактное хранилище состояния.

    Позволяет сохранять и получать состояние.
    Способ хранения состояния может варьироваться в зависимости
    от итоговой реализации. Например, можно хранить информацию
    в базе данных или в распределённом файловом хранилище.
    """

    @abc.abstractmethod
    def save_state(self, state: Dict[str, Any]) -> None:
        """Сохранить состояние в хранилище."""

    @abc.abstractmethod
    def retrieve_state(self) -> Dict[str, Any]:
        """Получить состояние из хранилища."""


class JsonFileStorage(BaseStorage):
    """Реализация хранилища, использующего локальный файл.

    Формат хранения: JSON
    """

    def __init__(self, file_path: str) -> None:
        self.file_path = file_path

    def save_state(self, state: Dict[str, Any]) -> None:
        with open(self.file_path, "w") as file:
            json.dump(state, file)

    def retrieve_state(self) -> Dict[str, Any]:
        if not os.path.exists(self.file_path):
            return {}
        with open(self.file_path, "r") as file:
            return json.load(file)


class RedisStorage(BaseStorage):
    def __init__(self, redis: "Redis"):
        self._redis = redis

    def save_state(self, state: Dict[str, Any]) -> None:
        self._redis.hset("data", mapping=state)

    def retrieve_state(self) -> Dict[str, Any]:
        bytes_dict = self._redis.hgetall("data")

        if not bytes_dict:
            return {}

        return {
            key.decode("utf-8"): value.decode("utf-8")
            for key, value in bytes_dict.items()
        }

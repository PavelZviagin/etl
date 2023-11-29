from abc import ABC, abstractmethod


class BaseTransform(ABC):
    def __init__(self, index_name):
        self.index_name = index_name

    @abstractmethod
    def transform(self, df: list[dict]) -> list[dict]:
        pass


class MoviesTransform(BaseTransform):

    def _get_persons_names_by_role(self, persons: list[dict], role: str) -> list[str]:  # noqa
        return [
            act["person_name"]
            for act in persons
            if act["person_role"] == role
        ]

    def get_persons_by_role(self, persons: list[dict], role: str) -> list[dict]:  # noqa
        return [
            {'id': act["person_id"], 'name': act["person_name"]}
            for act in persons if act["person_role"] == role
        ]

    def transform(self, data: list[dict]) -> list[dict]:
        transformed_data = []
        for row in data:
            transformed_data.append(
                {
                    "_index": self.index_name,
                    "_id": row["id"],
                    "id": row["id"],
                    "imdb_rating": row["rating"],
                    "genres": [
                        dict(id=act["genre_id"], name=act["genre_name"])
                        for act in row["genres"]
                    ],
                    "title": row["title"],
                    "description": row["description"],
                    "director": ",".join(
                        self._get_persons_names_by_role(row['persons'], 'director')
                    ),
                    "actors_names": ",".join(
                        self._get_persons_names_by_role(row['persons'], 'actor')
                    ),
                    "writers_names": [
                        self._get_persons_names_by_role(row['persons'], 'writer')
                    ],
                    "actors": [
                        self.get_persons_by_role(row['persons'], 'actor')
                    ],
                    "writers": [
                        self.get_persons_by_role(row['persons'], 'writer')
                    ],
                }
            )
        return transformed_data


class GenresTransform(BaseTransform):

    def transform(self, data: list[dict]) -> list[dict]:
        transformed_data = []
        for row in data:
            transformed_data.append(
                {
                    "_index": self.index_name,
                    "_id": row["id"],
                    "id": row["id"],
                    "name": row["name"],
                }
            )
        return transformed_data


class PersonsTransform(BaseTransform):

    def transform(self, data: list[dict]) -> list[dict]:
        transformed_data = []
        for row in data:
            transformed_data.append(
                {
                    "_index": self.index_name,
                    "_id": row["id"],
                    "id": row["id"],
                    "full_name": row["full_name"],
                    "roles": [role for role in row["roles"]],
                    "film_ids": [dict(id=film["film_id"], role=film["role"]) for film in row["films"]]
                }
            )
        return transformed_data


class TransformFactory:
    @staticmethod
    def get_transformer(index_name) -> BaseTransform:
        match index_name:
            case "genres":
                return GenresTransform(index_name)
            case "movies":
                return MoviesTransform(index_name)
            case "persons":
                return PersonsTransform(index_name)
            case _:
                raise ValueError(f"Unknown index")

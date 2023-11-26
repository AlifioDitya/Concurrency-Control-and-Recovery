from typing import Dict, List

from .Query import Query
from .BinaryData import Data

class Transaction:
    def __init__(self, start_timestamp: int, list_of_queries: List[Query]) -> None:
        self.start_timestamp = start_timestamp
        self.list_of_queries = list_of_queries
        self.query_index = 0
        self.dict_data: Dict[str, Data] = {}

    @property
    def length(self) -> int:
        return len(self.list_of_queries)

    @property
    def current_query(self) -> Query:
        return self.list_of_queries[self.query_index]

    def is_finished(self) -> bool:
        return self.query_index == self.length

    def next_query(self) -> None:
        self.query_index += 1

    def rollback(self, new_timestamp: int) -> None:
        self.start_timestamp = new_timestamp
        self.query_index = 0
        self.dict_data = {}

    def commit(self) -> None:
        for query in self.list_of_queries:
            current_file_names = query.get_file_names()
            current_data = [self.dict_data.setdefault(filename, Data()) for filename in current_file_names]
            query.execute(*current_data)

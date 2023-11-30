from typing import List

from .Query import Query

class Transaction:
    def __init__(self, start_timestamp: int, list_of_queries: List[Query]) -> None:
        self.start_timestamp = start_timestamp
        self.list_of_queries = list_of_queries
        self.query_index = 0

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
        print(f"T{self.start_timestamp} rolled back.")
        print()
        self.start_timestamp = new_timestamp
        self.query_index = 0

    def commit(self) -> None:
        print(f"T{self.start_timestamp} committed.")
        print()

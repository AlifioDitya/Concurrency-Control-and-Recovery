from __future__ import annotations
from typing import List, Set
import sys

from ConcurrencyControl import ConcurrencyControl
from transaction.Transaction import Transaction
from transaction.Query import Query, ReadQuery, WriteQuery, DisplayQuery, FunctionQuery


class OCCTransaction(Transaction):
    '''
    Transaction for Optimistic Concurrency Control

    Attributes:
        start_timestamp (int): The start timestamp of the transaction.
        list_of_queries (List[Query]): The list of queries in the transaction.
        end_timestamp (int): The end timestamp of the transaction.
        data_item_written (Set[str]): The set of data items written in the transaction.
        data_item_read (Set[str]): The set of data items read in the transaction.

    Methods:
        validation_test(counter_timestamp: int, other: OCCTransaction) -> bool:
            Returns True if the transaction is valid, False otherwise.
        next_query() -> None:
            Executes the next query in the transaction.
        rollback(new_timestamp: int) -> None:
            Rollbacks the transaction to the new timestamp.
    '''
    def __init__(self, start_timestamp: int, list_of_queries: List[Query]) -> None:
        super().__init__(start_timestamp, list_of_queries)

        # Adding end timestamp
        self.end_timestamp: int = sys.maxsize

        # Recording data items written and data items read
        self.data_item_written: Set[str] = set()
        self.data_item_read: Set[str] = set()

    def validation_test(self, counter_timestamp: int, other: OCCTransaction) -> bool:
        if self.start_timestamp <= other.start_timestamp:
            return True
        if self.start_timestamp >= other.end_timestamp:
            return True
        if self.start_timestamp < other.end_timestamp and counter_timestamp >= other.end_timestamp and not self.data_item_read.intersection(other.data_item_written):
            return True
        return False

    def next_query(self) -> None:
        current_query = self.current_query
        if isinstance(current_query, WriteQuery):
            for filename in current_query.get_file_names():
                self.data_item_written.add(filename)
                print(f"Write to {filename} in Transaction {self.start_timestamp}")

        if isinstance(current_query, ReadQuery):
            for filename in current_query.get_file_names():
                self.data_item_read.add(filename)
                print(f"Read from {filename} in Transaction {self.start_timestamp}")

        if isinstance(current_query, (DisplayQuery, FunctionQuery)):
            query_type = "DISPLAY" if isinstance(current_query, DisplayQuery) else "FUNCTION"
            print(
                f"{query_type} executed in Transaction {self.start_timestamp}"
            )

        super().next_query()

    def rollback(self, new_timestamp: int) -> None:
        self.data_item_written = set()
        self.data_item_read = set()

        super().rollback(new_timestamp)


class OCC(ConcurrencyControl):
    '''
    Optimistic Concurrency Control

    Attributes:
        list_of_transactions (List[OCCTransaction]): The list of transactions.
        schedule (List[int]): The schedule of the transactions.

    Methods:
        run() -> None:
            Runs the OCC algorithm.
    '''
    def __init__(self, list_of_transactions: List[OCCTransaction], schedule: List[int]) -> None:
        super().__init__(list_of_transactions, schedule)

    def run(self):
        temp_schedule: List[int] = [timestamp for timestamp in self.schedule]
        active_timestamp: List[int] = []
        counter = 0

        while temp_schedule:
            current_timestamp = temp_schedule.pop(0)

            if current_timestamp not in active_timestamp:
                active_timestamp.append(current_timestamp)
                print(f"Transaction {current_timestamp} started.")

            transaction: OCCTransaction = self.get_transaction(
                current_timestamp
            )

            transaction.next_query()

            if transaction.is_finished():
                valid = all(transaction.validation_test(
                    current_timestamp +
                    counter, self.get_transaction(timestamp)
                ) for timestamp in active_timestamp)

                if valid:
                    print(
                        f"Transaction {transaction.start_timestamp} committed."
                    )
                    print("RESULT:")
                    transaction.commit()
                    transaction.end_timestamp = current_timestamp + counter

                else:
                    temp_schedule = [ts for ts in temp_schedule if ts != current_timestamp]
                    active_timestamp = [ts for ts in active_timestamp if ts != current_timestamp]
                    new_timestamp = current_timestamp + counter + len(temp_schedule)

                    print(f"Transaction {current_timestamp} rolled back.")
                    transaction.rollback(new_timestamp)

                    temp_schedule.extend(
                        new_timestamp for _ in range(transaction.length)
                    )

            counter += 1

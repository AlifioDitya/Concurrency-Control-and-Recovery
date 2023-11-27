from typing import List
from transaction.Transaction import Transaction

class ConcurrencyControl:
    '''
    Concurrency Control Abstract Class.

    Attributes:
        transactions (List[Transaction]): The list of transactions.
        schedule (List[int]): The schedule of the transactions.

    Methods:
        get_transaction(start_timestamp: int) -> Transaction:
            Returns the transaction with the given start timestamp.
        run() -> None:
            Runs the concurrency control algorithm.
    '''
    def __init__(self, transactions: List[Transaction], schedule: List[int]) -> None:
        self.transactions = transactions
        self.schedule = schedule

    def get_transaction(self, start_timestamp: int) -> Transaction:
        return next(transaction for transaction in self.transactions if transaction.start_timestamp == start_timestamp)

    def run(self):
        raise NotImplementedError()

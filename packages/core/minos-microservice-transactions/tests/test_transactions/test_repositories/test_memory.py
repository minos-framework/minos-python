import unittest

from minos.transactions import (
    InMemoryTransactionRepository,
    TransactionRepository,
)
from minos.transactions.testing import (
    TransactionRepositoryTestCase,
)
from tests.utils import (
    TransactionsTestCase,
)


class TestInMemoryTransactionRepository(TransactionsTestCase, TransactionRepositoryTestCase):
    __test__ = True

    def build_transaction_repository(self) -> TransactionRepository:
        return InMemoryTransactionRepository()


if __name__ == "__main__":
    unittest.main()

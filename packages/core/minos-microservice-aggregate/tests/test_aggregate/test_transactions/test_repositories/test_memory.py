import unittest

from minos.aggregate import (
    InMemoryTransactionRepository,
    TransactionRepository,
)
from minos.aggregate.testing import (
    TransactionRepositorySelectTestCase,
    TransactionRepositorySubmitTestCase,
)
from tests.utils import (
    AggregateTestCase,
)


class TestInMemoryTransactionRepository(AggregateTestCase, TransactionRepositorySubmitTestCase):
    __test__ = True

    def build_transaction_repository(self) -> TransactionRepository:
        return InMemoryTransactionRepository()


class TestInMemoryTransactionRepositorySelect(AggregateTestCase, TransactionRepositorySelectTestCase):
    __test__ = True

    def build_transaction_repository(self) -> TransactionRepository:
        return InMemoryTransactionRepository()


if __name__ == "__main__":
    unittest.main()

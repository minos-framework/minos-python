import unittest

from minos.aggregate import (
    InMemoryTransactionRepository,
    TransactionRepository,
)
from minos.aggregate.testing import (
    TransactionRepositoryTestCase,
)
from tests.utils import (
    AggregateTestCase,
)


class TestInMemoryTransactionRepository(AggregateTestCase, TransactionRepositoryTestCase):
    __test__ = True

    def build_transaction_repository(self) -> TransactionRepository:
        return InMemoryTransactionRepository()


if __name__ == "__main__":
    unittest.main()

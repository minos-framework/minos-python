import unittest

from minos.common import (
    MinosException,
)
from minos.transactions import (
    TransactionRepositoryConflictException,
    TransactionRepositoryException,
)
from tests.utils import (
    TransactionsTestCase,
)


class TestExceptions(TransactionsTestCase):
    def test_transaction(self):
        self.assertTrue(issubclass(TransactionRepositoryException, MinosException))

    def test_transaction_conflict(self):
        message = "There was a conflict"
        exception = TransactionRepositoryConflictException(message)

        self.assertIsInstance(exception, TransactionRepositoryException)
        self.assertEqual(message, str(exception))


if __name__ == "__main__":
    unittest.main()

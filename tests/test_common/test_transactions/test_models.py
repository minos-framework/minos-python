import unittest
from uuid import (
    UUID,
)

from minos.common import (
    Transaction,
    TransactionStatus,
)


class TestTransaction(unittest.IsolatedAsyncioTestCase):
    def test_constructor(self):
        transaction = Transaction()

        self.assertIsInstance(transaction.uuid, UUID)
        self.assertEqual(TransactionStatus.CREATED, transaction.status)
        self.assertEqual(None, transaction.event_offset)


class TestTransactionStatus(unittest.TestCase):
    def test_value_of_created(self):
        self.assertEqual(TransactionStatus.CREATED, TransactionStatus.value_of("created"))

    def test_value_of_pending(self):
        self.assertEqual(TransactionStatus.PENDING, TransactionStatus.value_of("pending"))

    def test_value_of_reserved(self):
        self.assertEqual(TransactionStatus.RESERVED, TransactionStatus.value_of("reserved"))

    def test_value_of_committed(self):
        self.assertEqual(TransactionStatus.COMMITTED, TransactionStatus.value_of("committed"))

    def test_value_of_rejected(self):
        self.assertEqual(TransactionStatus.REJECTED, TransactionStatus.value_of("rejected"))

    def test_value_of_raises(self):
        with self.assertRaises(ValueError):
            TransactionStatus.value_of("foo")


if __name__ == "__main__":
    unittest.main()

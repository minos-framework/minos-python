import unittest
from abc import (
    ABC,
)

from minos.common import (
    DatabaseOperationFactory,
)
from minos.networks import (
    BrokerPublisherTransactionDatabaseOperationFactory,
)


class TestBrokerPublisherTransactionDatabaseOperationFactory(unittest.IsolatedAsyncioTestCase):
    def test_abstract(self):
        self.assertTrue(issubclass(BrokerPublisherTransactionDatabaseOperationFactory, (DatabaseOperationFactory, ABC)))

        # noinspection PyUnresolvedReferences
        self.assertEqual(
            {"build_submit", "build_delete_batch", "build_create", "build_query"},
            BrokerPublisherTransactionDatabaseOperationFactory.__abstractmethods__,
        )


if __name__ == "__main__":
    unittest.main()

import unittest

from minos.common.testing import (
    DatabaseMinosTestCase,
)
from minos.networks import (
    BrokerPublisherQueue,
    BrokerPublisherQueueDatabaseOperationFactory,
    DatabaseBrokerPublisherQueue,
    DatabaseBrokerQueue,
)
from tests.utils import (
    NetworksTestCase,
)


class TestDatabaseBrokerPublisherQueue(NetworksTestCase, DatabaseMinosTestCase):
    def test_is_subclass(self):
        self.assertTrue(issubclass(DatabaseBrokerPublisherQueue, (DatabaseBrokerQueue, BrokerPublisherQueue)))

    async def test_operation_factory(self):
        queue = DatabaseBrokerPublisherQueue.from_config(self.config)

        self.assertIsInstance(queue.operation_factory, BrokerPublisherQueueDatabaseOperationFactory)


if __name__ == "__main__":
    unittest.main()

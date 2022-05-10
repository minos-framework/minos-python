import unittest
from unittest.mock import (
    patch,
)
from uuid import (
    UUID,
)

from minos.aggregate import (
    Aggregate,
)
from minos.common import (
    Config,
    NotProvidedException,
)
from minos.networks import (
    InMemoryBrokerPublisher,
)
from tests.utils import (
    CONFIG_FILE_PATH,
    AggregateTestCase,
    Order,
    OrderAggregate,
)


class TestAggregate(AggregateTestCase):
    async def test_root(self):
        async with OrderAggregate.from_config(CONFIG_FILE_PATH) as aggregate:
            self.assertEqual(Order, aggregate.root)

    def test_root_raises(self):
        with self.assertRaises(TypeError):
            Aggregate.from_config(CONFIG_FILE_PATH)

    async def test_from_config(self):
        async with OrderAggregate.from_config(CONFIG_FILE_PATH) as aggregate:
            self.assertEqual(self.transaction_repository, aggregate.transaction_repository)
            self.assertEqual(self.event_repository, aggregate.event_repository)
            self.assertEqual(self.snapshot_repository, aggregate.snapshot_repository)
            self.assertEqual(self.broker_publisher, aggregate.broker_publisher)

    async def test_from_config_with_custom_publisher(self):
        with patch.object(Config, "get_aggregate", return_value={"publisher": {"client": InMemoryBrokerPublisher}}):
            async with OrderAggregate.from_config(CONFIG_FILE_PATH) as aggregate:
                self.assertEqual(self.transaction_repository, aggregate.transaction_repository)
                self.assertEqual(self.event_repository, aggregate.event_repository)
                self.assertEqual(self.snapshot_repository, aggregate.snapshot_repository)
                self.assertIsInstance(aggregate.broker_publisher, InMemoryBrokerPublisher)
                self.assertNotEqual(self.broker_publisher, aggregate.broker_publisher)

    def test_from_config_raises(self):
        with self.assertRaises(NotProvidedException):
            OrderAggregate.from_config(CONFIG_FILE_PATH, transaction_repository=None)
        with self.assertRaises(NotProvidedException):
            OrderAggregate.from_config(CONFIG_FILE_PATH, event_repository=None)
        with self.assertRaises(NotProvidedException):
            OrderAggregate.from_config(CONFIG_FILE_PATH, snapshot_repository=None)
        with self.assertRaises(NotProvidedException):
            OrderAggregate.from_config(CONFIG_FILE_PATH, broker_publisher=None)

    async def test_call(self):
        async with OrderAggregate.from_config(CONFIG_FILE_PATH) as aggregate:
            uuid = await aggregate.create_order()
        self.assertIsInstance(uuid, UUID)


if __name__ == "__main__":
    unittest.main()

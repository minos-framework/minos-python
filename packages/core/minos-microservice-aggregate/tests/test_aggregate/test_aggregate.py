import unittest
from uuid import (
    UUID,
)

from minos.aggregate import (
    Aggregate,
)
from minos.common import (
    NotProvidedException,
)
from tests.utils import (
    CONFIG_FILE_PATH,
    MinosTestCase,
    Order,
    OrderAggregate,
)


class TestAggregate(MinosTestCase):
    async def test_root(self):
        async with OrderAggregate.from_config(CONFIG_FILE_PATH) as aggregate:
            self.assertEqual(Order, aggregate.root)

    def test_root_raises(self):
        with self.assertRaises(TypeError):
            Aggregate.from_config(CONFIG_FILE_PATH)

    async def test_from_config(self):
        async with OrderAggregate.from_config(CONFIG_FILE_PATH) as aggregate:
            self.assertTrue(self.transaction_repository, aggregate.transaction_repository)
            self.assertTrue(self.event_repository, aggregate.event_repository)
            self.assertTrue(self.snapshot_repository, aggregate.snapshot_repository)

    def test_from_config_raises(self):
        with self.assertRaises(NotProvidedException):
            OrderAggregate.from_config(CONFIG_FILE_PATH, transaction_repository=None)
        with self.assertRaises(NotProvidedException):
            OrderAggregate.from_config(CONFIG_FILE_PATH, event_repository=None)
        with self.assertRaises(NotProvidedException):
            OrderAggregate.from_config(CONFIG_FILE_PATH, snapshot_repository=None)

    async def test_call(self):
        async with OrderAggregate.from_config(CONFIG_FILE_PATH) as aggregate:
            uuid = await aggregate.create_order()
        self.assertIsInstance(uuid, UUID)


if __name__ == "__main__":
    unittest.main()

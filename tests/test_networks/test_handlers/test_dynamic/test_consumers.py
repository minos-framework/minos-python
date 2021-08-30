"""tests.test_networks.test_handlers.test_dynamic.test_consumers module."""

import unittest

from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    DynamicConsumer,
)
from tests.utils import (
    BASE_PATH,
)


class TestDynamicConsumer(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def test_from_config(self):
        consumer = DynamicConsumer.from_config(config=self.config)
        self.assertIsInstance(consumer, DynamicConsumer)
        self.assertEqual(set(), consumer.topics)
        self.assertEqual(self.config.broker, consumer._broker)
        self.assertEqual(self.config.broker.queue.host, consumer.host)
        self.assertEqual(self.config.broker.queue.port, consumer.port)
        self.assertEqual(self.config.broker.queue.database, consumer.database)
        self.assertEqual(self.config.broker.queue.user, consumer.user)
        self.assertEqual(self.config.broker.queue.password, consumer.password)

    def test_table_name(self):
        self.assertEqual("dynamic_queue", DynamicConsumer.TABLE_NAME)


if __name__ == "__main__":
    unittest.main()

import unittest

from minos.networks import (
    InMemoryBrokerSubscriberQueue,
    InMemoryQueuedKafkaBrokerSubscriber,
    KafkaBrokerSubscriber,
    PostgreSqlBrokerSubscriberQueue,
    PostgreSqlQueuedKafkaBrokerSubscriber,
)
from tests.utils import (
    CONFIG_FILE_PATH,
)


class TestPostgreSqlQueuedKafkaBrokerSubscriber(unittest.IsolatedAsyncioTestCase):
    def test_from_config(self):
        subscriber = PostgreSqlQueuedKafkaBrokerSubscriber.from_config(CONFIG_FILE_PATH, topics={"foo", "bar"})
        self.assertIsInstance(subscriber, PostgreSqlQueuedKafkaBrokerSubscriber)
        self.assertIsInstance(subscriber.impl, KafkaBrokerSubscriber)
        self.assertIsInstance(subscriber.queue, PostgreSqlBrokerSubscriberQueue)


class TestInMemoryQueuedKafkaBrokerSubscriber(unittest.IsolatedAsyncioTestCase):
    def test_from_config(self):
        subscriber = InMemoryQueuedKafkaBrokerSubscriber.from_config(CONFIG_FILE_PATH, topics={"foo", "bar"})
        self.assertIsInstance(subscriber, InMemoryQueuedKafkaBrokerSubscriber)
        self.assertIsInstance(subscriber.impl, KafkaBrokerSubscriber)
        self.assertIsInstance(subscriber.queue, InMemoryBrokerSubscriberQueue)


if __name__ == "__main__":
    unittest.main()

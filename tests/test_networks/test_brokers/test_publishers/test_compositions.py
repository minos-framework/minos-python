import unittest

from minos.networks import (
    InMemoryBrokerPublisherRepository,
    InMemoryQueuedKafkaBrokerPublisher,
    KafkaBrokerPublisher,
    PostgreSqlBrokerPublisherRepository,
    PostgreSqlQueuedKafkaBrokerPublisher,
)
from tests.utils import (
    CONFIG_FILE_PATH,
)


class TestPostgreSqlQueuedKafkaBrokerPublisher(unittest.IsolatedAsyncioTestCase):
    def test_from_config(self):
        publisher = PostgreSqlQueuedKafkaBrokerPublisher.from_config(CONFIG_FILE_PATH)
        self.assertIsInstance(publisher, PostgreSqlQueuedKafkaBrokerPublisher)
        self.assertIsInstance(publisher.impl, KafkaBrokerPublisher)
        self.assertIsInstance(publisher.repository, PostgreSqlBrokerPublisherRepository)


class TestInMemoryQueuedKafkaBrokerPublisher(unittest.IsolatedAsyncioTestCase):
    def test_from_config(self):
        publisher = InMemoryQueuedKafkaBrokerPublisher.from_config(CONFIG_FILE_PATH)
        self.assertIsInstance(publisher, InMemoryQueuedKafkaBrokerPublisher)
        self.assertIsInstance(publisher.impl, KafkaBrokerPublisher)
        self.assertIsInstance(publisher.repository, InMemoryBrokerPublisherRepository)


if __name__ == "__main__":
    unittest.main()

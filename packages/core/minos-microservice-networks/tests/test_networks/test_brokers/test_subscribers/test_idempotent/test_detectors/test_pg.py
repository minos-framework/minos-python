import unittest

from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    BrokerMessageV1,
    BrokerMessageV1Payload,
    BrokerSubscriberDuplicateDetector,
    PostgreSqlBrokerSubscriberDuplicateDetector,
    PostgreSqlBrokerSubscriberDuplicateDetectorQueryFactory,
)
from tests.utils import (
    CONFIG_FILE_PATH,
)


class TestPostgreSqlBrokerSubscriberDuplicateDetector(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = CONFIG_FILE_PATH

    def test_is_subclass(self):
        self.assertTrue(issubclass(PostgreSqlBrokerSubscriberDuplicateDetector, BrokerSubscriberDuplicateDetector))

    async def test_query_factory(self):
        detector = PostgreSqlBrokerSubscriberDuplicateDetector.from_config(self.config)

        self.assertIsInstance(detector.query_factory, PostgreSqlBrokerSubscriberDuplicateDetectorQueryFactory)

    async def test_is_valid(self):
        one = BrokerMessageV1("foo", BrokerMessageV1Payload("bar"))
        two = BrokerMessageV1("foo", BrokerMessageV1Payload("bar"))
        three = BrokerMessageV1("foo", BrokerMessageV1Payload("bar"))

        async with PostgreSqlBrokerSubscriberDuplicateDetector.from_config(self.config) as detector:
            self.assertTrue(await detector.is_valid(one))
            self.assertTrue(await detector.is_valid(two))
            self.assertFalse(await detector.is_valid(one))
            self.assertTrue(await detector.is_valid(three))


if __name__ == "__main__":
    unittest.main()

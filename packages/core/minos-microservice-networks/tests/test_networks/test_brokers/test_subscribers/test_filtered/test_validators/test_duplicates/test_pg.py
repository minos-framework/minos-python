import unittest

from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    BrokerMessageV1,
    BrokerMessageV1Payload,
    BrokerSubscriberValidator,
    PostgreSqlBrokerSubscriberDuplicateValidator,
    PostgreSqlBrokerSubscriberDuplicateValidatorQueryFactory,
)
from tests.utils import (
    CONFIG_FILE_PATH,
)


class TestPostgreSqlBrokerSubscriberDuplicateValidator(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = CONFIG_FILE_PATH

    def test_is_subclass(self):
        self.assertTrue(issubclass(PostgreSqlBrokerSubscriberDuplicateValidator, BrokerSubscriberValidator))

    async def test_query_factory(self):
        validator = PostgreSqlBrokerSubscriberDuplicateValidator.from_config(self.config)

        self.assertIsInstance(validator.query_factory, PostgreSqlBrokerSubscriberDuplicateValidatorQueryFactory)

    async def test_is_valid(self):
        one = BrokerMessageV1("foo", BrokerMessageV1Payload("bar"))
        two = BrokerMessageV1("foo", BrokerMessageV1Payload("bar"))
        three = BrokerMessageV1("foo", BrokerMessageV1Payload("bar"))

        async with PostgreSqlBrokerSubscriberDuplicateValidator.from_config(self.config) as validator:
            self.assertTrue(await validator.is_valid(one))
            self.assertTrue(await validator.is_valid(two))
            self.assertFalse(await validator.is_valid(one))
            self.assertTrue(await validator.is_valid(three))


if __name__ == "__main__":
    unittest.main()

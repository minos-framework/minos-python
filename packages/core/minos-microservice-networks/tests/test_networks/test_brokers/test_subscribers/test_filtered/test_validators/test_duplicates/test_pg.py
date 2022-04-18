import unittest

from minos.common.testing import (
    DatabaseMinosTestCase,
)
from minos.networks import (
    AiopgBrokerSubscriberDuplicateValidatorDatabaseOperationFactory,
    BrokerMessageV1,
    BrokerMessageV1Payload,
    BrokerSubscriberValidator,
    DatabaseBrokerSubscriberDuplicateValidator,
)
from tests.utils import (
    NetworksTestCase,
)


class TestDatabaseBrokerSubscriberDuplicateValidator(NetworksTestCase, DatabaseMinosTestCase):
    def test_is_subclass(self):
        self.assertTrue(issubclass(DatabaseBrokerSubscriberDuplicateValidator, BrokerSubscriberValidator))

    async def test_operation_factory(self):
        validator = DatabaseBrokerSubscriberDuplicateValidator.from_config(self.config)

        self.assertIsInstance(
            validator.operation_factory, AiopgBrokerSubscriberDuplicateValidatorDatabaseOperationFactory
        )

    async def test_is_valid(self):
        one = BrokerMessageV1("foo", BrokerMessageV1Payload("bar"))
        two = BrokerMessageV1("foo", BrokerMessageV1Payload("bar"))
        three = BrokerMessageV1("foo", BrokerMessageV1Payload("bar"))

        async with DatabaseBrokerSubscriberDuplicateValidator.from_config(self.config) as validator:
            self.assertTrue(await validator.is_valid(one))
            self.assertTrue(await validator.is_valid(two))
            self.assertFalse(await validator.is_valid(one))
            self.assertTrue(await validator.is_valid(three))


if __name__ == "__main__":
    unittest.main()

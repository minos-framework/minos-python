import unittest
from unittest.mock import (
    patch,
)

from minos.common import (
    IntegrityException,
)
from minos.common.testing import (
    DatabaseMinosTestCase,
    MockedDatabaseClient,
)
from minos.networks import (
    BrokerMessageV1,
    BrokerMessageV1Payload,
    BrokerSubscriberValidator,
    DatabaseBrokerSubscriberDuplicateValidator,
)
from minos.networks.testing import (
    MockedBrokerSubscriberDuplicateValidatorDatabaseOperationFactory,
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
            validator.operation_factory, MockedBrokerSubscriberDuplicateValidatorDatabaseOperationFactory
        )

    async def test_is_valid(self):
        one = BrokerMessageV1("foo", BrokerMessageV1Payload("bar"))
        two = BrokerMessageV1("foo", BrokerMessageV1Payload("bar"))
        three = BrokerMessageV1("foo", BrokerMessageV1Payload("bar"))

        with patch.object(
            MockedDatabaseClient, "execute", side_effect=[None, None, None, IntegrityException(""), None]
        ):
            async with DatabaseBrokerSubscriberDuplicateValidator.from_config(self.config) as validator:
                self.assertTrue(await validator.is_valid(one))
                self.assertTrue(await validator.is_valid(two))
                self.assertFalse(await validator.is_valid(one))
                self.assertTrue(await validator.is_valid(three))


if __name__ == "__main__":
    unittest.main()

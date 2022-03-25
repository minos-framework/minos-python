import unittest
from abc import (
    ABC,
)
from unittest.mock import (
    AsyncMock,
    call,
)

from minos.common import (
    SetupMixin,
)
from minos.networks import (
    BrokerMessage,
    BrokerMessageV1,
    BrokerMessageV1Payload,
    BrokerSubscriberValidator,
)


class _BrokerSubscriberValidator(BrokerSubscriberValidator):
    """For testing purposes."""

    async def _is_valid(self, message: BrokerMessage) -> bool:
        """For testing purposes."""


class TestBrokerSubscriberValidator(unittest.IsolatedAsyncioTestCase):
    def test_abstract(self):
        self.assertTrue(issubclass(BrokerSubscriberValidator, (ABC, SetupMixin)))
        # noinspection PyUnresolvedReferences
        self.assertEqual({"_is_valid"}, BrokerSubscriberValidator.__abstractmethods__)

    async def test_is_valid(self):
        message = BrokerMessageV1("foo", BrokerMessageV1Payload("bar"))
        validator = _BrokerSubscriberValidator()

        mock = AsyncMock(side_effect=[True, False])
        validator._is_valid = mock

        self.assertTrue(await validator.is_valid(message))
        self.assertFalse(await validator.is_valid(message))

        self.assertEqual([call(message), call(message)], mock.call_args_list)


if __name__ == "__main__":
    unittest.main()

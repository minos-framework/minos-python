import unittest
from abc import (
    ABC,
)
from unittest.mock import (
    AsyncMock,
    call,
)
from uuid import (
    UUID,
)

from minos.networks import (
    BrokerMessageV1,
    BrokerMessageV1Payload,
    BrokerSubscriberDuplicateValidator,
    BrokerSubscriberValidator,
)


class _BrokerSubscriberDuplicateValidator(BrokerSubscriberDuplicateValidator):
    """For testing purposes."""

    async def _is_unique(self, topic: str, uuid: UUID) -> bool:
        """For testing purposes."""


class TestBrokerSubscriberDuplicateValidator(unittest.IsolatedAsyncioTestCase):
    def test_abstract(self):
        self.assertTrue(issubclass(BrokerSubscriberDuplicateValidator, (ABC, BrokerSubscriberValidator)))
        # noinspection PyUnresolvedReferences
        self.assertEqual({"_is_unique"}, BrokerSubscriberDuplicateValidator.__abstractmethods__)

    async def test_is_unique(self):
        message = BrokerMessageV1("foo", BrokerMessageV1Payload("bar"))
        validator = _BrokerSubscriberDuplicateValidator()

        mock = AsyncMock(side_effect=[True, False])
        validator._is_unique = mock

        self.assertTrue(await validator.is_valid(message))
        self.assertFalse(await validator.is_valid(message))

        self.assertEqual(
            [call(message.topic, message.identifier), call(message.topic, message.identifier)], mock.call_args_list
        )


if __name__ == "__main__":
    unittest.main()

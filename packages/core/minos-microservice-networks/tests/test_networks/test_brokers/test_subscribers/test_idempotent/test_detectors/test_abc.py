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

from minos.common import (
    SetupMixin,
)
from minos.networks import (
    BrokerMessageV1,
    BrokerMessageV1Payload,
    BrokerSubscriberDuplicateDetector,
)


class _BrokerSubscriberDuplicateDetector(BrokerSubscriberDuplicateDetector):
    """For testing purposes."""

    async def _is_valid(self, topic: str, uuid: UUID) -> bool:
        """For testing purposes."""


class TestBrokerSubscriberDuplicateDetector(unittest.IsolatedAsyncioTestCase):
    def test_abstract(self):
        self.assertTrue(issubclass(BrokerSubscriberDuplicateDetector, (ABC, SetupMixin)))
        # noinspection PyUnresolvedReferences
        self.assertEqual({"_is_valid"}, BrokerSubscriberDuplicateDetector.__abstractmethods__)

    async def test_is_valid(self):
        message = BrokerMessageV1("foo", BrokerMessageV1Payload("bar"))
        detector = _BrokerSubscriberDuplicateDetector()

        mock = AsyncMock(side_effect=[True, False])
        detector._is_valid = mock

        self.assertTrue(await detector.is_valid(message))
        self.assertFalse(await detector.is_valid(message))

        self.assertEqual(
            [call(message.topic, message.identifier), call(message.topic, message.identifier)], mock.call_args_list
        )


if __name__ == "__main__":
    unittest.main()

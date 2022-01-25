import unittest
from abc import (
    ABC,
)
from unittest.mock import (
    AsyncMock,
    call,
)

from minos.common import (
    MinosSetup,
)
from minos.networks import (
    BrokerMessage,
    BrokerMessageV1,
    BrokerMessageV1Payload,
    BrokerPublisher,
)


class _BrokerPublisher(BrokerPublisher):
    """For testing purposes."""

    async def _send(self, message: BrokerMessage) -> None:
        """For testing purposes."""


class TestBrokerPublisher(unittest.IsolatedAsyncioTestCase):
    def test_abstract(self):
        self.assertTrue(issubclass(BrokerPublisher, (ABC, MinosSetup)))
        # noinspection PyUnresolvedReferences
        self.assertEqual(
            {"_send"}, BrokerPublisher.__abstractmethods__,
        )

    async def test_send(self):
        publisher = _BrokerPublisher()
        mock = AsyncMock()
        publisher._send = mock

        message = BrokerMessageV1("foo", BrokerMessageV1Payload("bar"))
        await publisher.send(message)

        self.assertEqual([call(message)], mock.call_args_list)


if __name__ == "__main__":
    unittest.main()

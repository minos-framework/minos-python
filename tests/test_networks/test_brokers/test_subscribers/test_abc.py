import unittest
from abc import (
    ABC,
)
from unittest.mock import (
    AsyncMock,
)

from minos.common import (
    MinosSetup,
)
from minos.networks import (
    BrokerMessage,
    BrokerMessageV1,
    BrokerMessageV1Payload,
    BrokerSubscriber,
)


class _BrokerSubscriber(BrokerSubscriber):
    async def receive(self) -> BrokerMessage:
        """For testing purposes."""


class TestBrokerSubscriber(unittest.IsolatedAsyncioTestCase):
    def test_abstract(self):
        self.assertTrue(issubclass(BrokerSubscriber, (ABC, MinosSetup)))
        # noinspection PyUnresolvedReferences
        self.assertEqual(
            {"receive"}, BrokerSubscriber.__abstractmethods__,
        )

    def test_topics(self):
        subscriber = _BrokerSubscriber(["foo", "bar", "bar"])
        self.assertEqual({"foo", "bar"}, subscriber.topics)

    async def test_aiter(self):
        expected = [
            BrokerMessageV1("foo", BrokerMessageV1Payload("bar")),
            BrokerMessageV1("bar", BrokerMessageV1Payload("foo")),
        ]
        receive_mock = AsyncMock(side_effect=expected)
        subscriber = _BrokerSubscriber(list())
        subscriber.receive = receive_mock

        await subscriber.setup()
        observed = list()
        async for message in subscriber:
            observed.append(message)
            if len(observed) == len(expected):
                await subscriber.destroy()

        self.assertEqual(expected, observed)


if __name__ == "__main__":
    unittest.main()

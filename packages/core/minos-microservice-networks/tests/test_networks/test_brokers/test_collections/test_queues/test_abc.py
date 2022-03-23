import unittest
from abc import (
    ABC,
)
from unittest.mock import (
    AsyncMock,
)

from minos.common import (
    SetupMixin,
)
from minos.networks import (
    BrokerMessage,
    BrokerMessageV1,
    BrokerMessageV1Payload,
    BrokerQueue,
)


class _BrokerQueue(BrokerQueue):
    """For testing purposes."""

    async def _enqueue(self, message: BrokerMessage) -> None:
        """For testing purposes."""

    async def _dequeue(self) -> BrokerMessage:
        """For testing purposes."""


class TestBrokerQueue(unittest.IsolatedAsyncioTestCase):
    def test_abstract(self):
        self.assertTrue(issubclass(BrokerQueue, (ABC, SetupMixin)))
        # noinspection PyUnresolvedReferences
        self.assertEqual({"_enqueue", "_dequeue"}, BrokerQueue.__abstractmethods__)

    async def test_iter(self):
        messages = [
            BrokerMessageV1("foo", BrokerMessageV1Payload("bar")),
            BrokerMessageV1("bar", BrokerMessageV1Payload("foo")),
        ]
        dequeue_mock = AsyncMock(side_effect=messages)

        async with _BrokerQueue() as queue:
            queue._dequeue = dequeue_mock
            observed = await queue.__aiter__().__anext__()

        self.assertEqual(messages[0], observed)
        self.assertEqual(1, dequeue_mock.call_count)

    async def test_iter_raises(self):
        messages = [
            BrokerMessageV1("foo", BrokerMessageV1Payload("bar")),
            BrokerMessageV1("bar", BrokerMessageV1Payload("foo")),
        ]
        dequeue_mock = AsyncMock(side_effect=messages)

        queue = _BrokerQueue()
        queue._dequeue = dequeue_mock
        with self.assertRaises(StopAsyncIteration):
            await queue.__aiter__().__anext__()


if __name__ == "__main__":
    unittest.main()

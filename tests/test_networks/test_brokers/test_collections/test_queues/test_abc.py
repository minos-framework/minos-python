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
    BrokerRepository,
)


class _BrokerRepository(BrokerRepository):
    """For testing purposes."""

    async def _enqueue(self, message: BrokerMessage) -> None:
        """For testing purposes."""

    async def _dequeue(self) -> BrokerMessage:
        """For testing purposes."""


class TestBrokerRepository(unittest.IsolatedAsyncioTestCase):
    def test_abstract(self):
        self.assertTrue(issubclass(BrokerRepository, (ABC, MinosSetup)))
        # noinspection PyUnresolvedReferences
        self.assertEqual(
            {"_enqueue", "_dequeue"}, BrokerRepository.__abstractmethods__,
        )

    async def test_iter(self):
        messages = [
            BrokerMessageV1("foo", BrokerMessageV1Payload("bar")),
            BrokerMessageV1("bar", BrokerMessageV1Payload("foo")),
        ]
        dequeue_mock = AsyncMock(side_effect=messages)

        async with _BrokerRepository() as repository:
            repository._dequeue = dequeue_mock
            observed = await repository.__aiter__().__anext__()

        self.assertEqual(messages[0], observed)
        self.assertEqual(1, dequeue_mock.call_count)

    async def test_iter_raises(self):
        messages = [
            BrokerMessageV1("foo", BrokerMessageV1Payload("bar")),
            BrokerMessageV1("bar", BrokerMessageV1Payload("foo")),
        ]
        dequeue_mock = AsyncMock(side_effect=messages)

        repository = _BrokerRepository()
        repository._dequeue = dequeue_mock
        with self.assertRaises(StopAsyncIteration):
            await repository.__aiter__().__anext__()


if __name__ == "__main__":
    unittest.main()

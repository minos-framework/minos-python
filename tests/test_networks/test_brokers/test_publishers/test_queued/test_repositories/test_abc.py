import unittest
from abc import (
    ABC,
)
from typing import (
    AsyncIterator,
)
from unittest.mock import (
    MagicMock,
)

from minos.common import (
    MinosSetup,
)
from minos.networks import (
    BrokerMessage,
    BrokerMessageV1,
    BrokerMessageV1Payload,
    BrokerPublisherRepository,
)
from tests.utils import (
    FakeAsyncIterator,
)


class _BrokerPublisherRepository(BrokerPublisherRepository):
    """For testing purposes."""

    async def enqueue(self, message: BrokerMessage) -> None:
        """For testing purposes."""

    def dequeue_all(self) -> AsyncIterator[BrokerMessage]:
        """For testing purposes."""


class TestBrokerPublisherRepository(unittest.IsolatedAsyncioTestCase):
    def test_abstract(self):
        self.assertTrue(issubclass(BrokerPublisherRepository, (ABC, MinosSetup)))
        # noinspection PyUnresolvedReferences
        self.assertEqual(
            {"enqueue", "dequeue_all"}, BrokerPublisherRepository.__abstractmethods__,
        )

    async def test_enqueue(self):
        messages = [
            BrokerMessageV1("foo", BrokerMessageV1Payload("bar")),
            BrokerMessageV1("bar", BrokerMessageV1Payload("foo")),
        ]
        dequeue_all_mock = MagicMock(return_value=FakeAsyncIterator(messages))

        repository = _BrokerPublisherRepository()
        repository.dequeue_all = dequeue_all_mock

        self.assertEqual(messages[0], await repository.dequeue())
        self.assertEqual(1, dequeue_all_mock.call_count)


if __name__ == "__main__":
    unittest.main()

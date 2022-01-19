import unittest
from unittest.mock import (
    AsyncMock,
    call,
)

from minos.networks import (
    BrokerMessageV1,
    BrokerMessageV1Payload,
    BrokerPublisherRepository,
    InMemoryBrokerPublisherRepository,
)


class TestInMemoryBrokerPublisherRepository(unittest.IsolatedAsyncioTestCase):
    def test_is_subclass(self):
        self.assertTrue(issubclass(InMemoryBrokerPublisherRepository, BrokerPublisherRepository))

    async def test_enqueue(self):
        put_mock = AsyncMock()

        repository = InMemoryBrokerPublisherRepository()
        repository.queue.put = put_mock

        message = BrokerMessageV1("foo", BrokerMessageV1Payload("bar"))

        await repository.enqueue(message)

        self.assertEqual([call(message)], put_mock.call_args_list)

    async def test_iter(self):
        messages = [
            BrokerMessageV1("foo", BrokerMessageV1Payload("bar")),
            BrokerMessageV1("bar", BrokerMessageV1Payload("foo")),
        ]

        get_mock = AsyncMock(side_effect=[messages[0], messages[1], InterruptedError])

        async with InMemoryBrokerPublisherRepository() as repository:
            repository.queue.get = get_mock

            observed = list()
            with self.assertRaises(InterruptedError):
                async for message in repository:
                    observed.append(message)

        self.assertEqual(messages, observed)


if __name__ == "__main__":
    unittest.main()

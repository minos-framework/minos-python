import unittest
from unittest.mock import (
    AsyncMock,
    call,
)

from minos.networks import (
    BrokerMessageV1,
    BrokerMessageV1Payload,
    BrokerSubscriberRepository,
    InMemoryBrokerSubscriberRepository,
    InMemoryBrokerSubscriberRepositoryBuilder,
)


class TestInMemoryBrokerSubscriberRepository(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.topics = {"foo", "bar"}

    def test_is_subclass(self):
        self.assertTrue(issubclass(InMemoryBrokerSubscriberRepository, BrokerSubscriberRepository))

    async def test_enqueue(self):

        message = BrokerMessageV1("foo", BrokerMessageV1Payload("bar"))

        async with InMemoryBrokerSubscriberRepository(self.topics) as repository:
            put_mock = AsyncMock(side_effect=repository._queue.put)
            repository._queue.put = put_mock

            await repository.enqueue(message)

        self.assertEqual([call(message)], put_mock.call_args_list)

    async def test_iter(self):
        messages = [
            BrokerMessageV1("foo", BrokerMessageV1Payload("bar")),
            BrokerMessageV1("bar", BrokerMessageV1Payload("foo")),
        ]
        repository = InMemoryBrokerSubscriberRepository(self.topics)
        await repository.setup()
        await repository.enqueue(messages[0])
        await repository.enqueue(messages[1])

        observed = list()
        async for message in repository:
            observed.append(message)
            if len(observed) == len(messages):
                await repository.destroy()

        self.assertEqual(messages, observed)


class TestInMemoryBrokerSubscriberRepositoryBuilder(unittest.TestCase):
    def test_build(self):
        builder = InMemoryBrokerSubscriberRepositoryBuilder().with_topics({"one", "two"})
        subscriber = builder.build()

        self.assertIsInstance(subscriber, InMemoryBrokerSubscriberRepository)
        self.assertEqual({"one", "two"}, subscriber.topics)


if __name__ == "__main__":
    unittest.main()

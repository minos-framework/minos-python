import unittest
from asyncio import (
    TimeoutError,
    wait_for,
)

from minos.networks import (
    BrokerMessageV1,
    BrokerMessageV1Payload,
    BrokerSubscriber,
    InMemoryBrokerSubscriber,
    InMemoryBrokerSubscriberBuilder,
)


class TestInMemoryBrokerSubscriber(unittest.IsolatedAsyncioTestCase):
    def test_is_subclass(self):
        self.assertTrue(issubclass(InMemoryBrokerSubscriber, BrokerSubscriber))

    async def test_receive(self):
        messages = [
            BrokerMessageV1("foo", BrokerMessageV1Payload("bar")),
            BrokerMessageV1("bar", BrokerMessageV1Payload("foo")),
        ]

        async with InMemoryBrokerSubscriber({"foo", "bar"}, messages) as subscriber:
            self.assertEqual(messages[0], await subscriber.receive())
            self.assertEqual(messages[1], await subscriber.receive())

            with self.assertRaises(TimeoutError):
                await wait_for(subscriber.receive(), 0.1)

    async def test_add_message(self):
        message = BrokerMessageV1("foo", BrokerMessageV1Payload("bar"))
        async with InMemoryBrokerSubscriber({"foo", "bar"}) as subscriber:
            subscriber.add_message(message)
            self.assertEqual(message, await subscriber.receive())


class TestInMemoryBrokerSubscriberBuilder(unittest.TestCase):
    def test_with_messages(self):
        messages = [
            BrokerMessageV1("foo", BrokerMessageV1Payload("bar")),
            BrokerMessageV1("bar", BrokerMessageV1Payload("foo")),
        ]
        builder = InMemoryBrokerSubscriberBuilder().with_messages(messages)

        self.assertIsInstance(builder, InMemoryBrokerSubscriberBuilder)
        self.assertEqual({"messages": messages}, builder.kwargs)

    def test_build(self):
        builder = InMemoryBrokerSubscriberBuilder().with_topics({"one", "two"})
        subscriber = builder.build()

        self.assertIsInstance(subscriber, InMemoryBrokerSubscriber)
        self.assertEqual({"one", "two"}, subscriber.topics)


if __name__ == "__main__":
    unittest.main()

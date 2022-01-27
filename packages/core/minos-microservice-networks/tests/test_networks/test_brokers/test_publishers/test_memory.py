import unittest

from minos.networks import (
    BrokerMessageV1,
    BrokerMessageV1Payload,
    BrokerPublisher,
    InMemoryBrokerPublisher,
)


class TestInMemoryBrokerPublisher(unittest.IsolatedAsyncioTestCase):
    def test_is_subclass(self):
        self.assertTrue(issubclass(InMemoryBrokerPublisher, BrokerPublisher))

    async def test_send(self):
        publisher = InMemoryBrokerPublisher()

        messages = [
            BrokerMessageV1("foo", BrokerMessageV1Payload("bar")),
            BrokerMessageV1("bar", BrokerMessageV1Payload("foo")),
        ]

        await publisher.send(messages[0])
        await publisher.send(messages[1])

        self.assertEqual(messages, publisher.messages)


if __name__ == "__main__":
    unittest.main()

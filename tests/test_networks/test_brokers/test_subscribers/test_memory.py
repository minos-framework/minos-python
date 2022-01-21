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
)


class TestInMemoryBrokerSubscriber(unittest.IsolatedAsyncioTestCase):
    def test_is_subclass(self):
        self.assertTrue(issubclass(InMemoryBrokerSubscriber, BrokerSubscriber))

    async def test_receive(self):
        messages = [
            BrokerMessageV1("foo", BrokerMessageV1Payload("bar")),
            BrokerMessageV1("bar", BrokerMessageV1Payload("foo")),
        ]

        async with InMemoryBrokerSubscriber(messages, {"foo", "bar"}) as subscriber:
            self.assertEqual(messages[0], await subscriber.receive())
            self.assertEqual(messages[1], await subscriber.receive())

            with self.assertRaises(TimeoutError):
                await wait_for(subscriber.receive(), 0.1)


if __name__ == "__main__":
    unittest.main()

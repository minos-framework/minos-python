import unittest
from unittest.mock import (
    AsyncMock,
    call,
)

from minos.networks import (
    BrokerMessageV1,
    BrokerMessageV1Payload,
    BrokerPublisherQueue,
    InMemoryBrokerPublisherQueue,
)


class TestInMemoryBrokerPublisherQueue(unittest.IsolatedAsyncioTestCase):
    def test_is_subclass(self):
        self.assertTrue(issubclass(InMemoryBrokerPublisherQueue, BrokerPublisherQueue))

    async def test_enqueue(self):

        message = BrokerMessageV1("foo", BrokerMessageV1Payload("bar"))

        async with InMemoryBrokerPublisherQueue() as queue:
            put_mock = AsyncMock(side_effect=queue._queue.put)
            queue._queue.put = put_mock

            await queue.enqueue(message)

        self.assertEqual([call(message)], put_mock.call_args_list)

    async def test_iter(self):
        messages = [
            BrokerMessageV1("foo", BrokerMessageV1Payload("bar")),
            BrokerMessageV1("bar", BrokerMessageV1Payload("foo")),
        ]
        queue = InMemoryBrokerPublisherQueue()
        await queue.setup()
        await queue.enqueue(messages[0])
        await queue.enqueue(messages[1])

        observed = list()
        async for message in queue:
            observed.append(message)
            if len(observed) == len(messages):
                await queue.destroy()

        self.assertEqual(messages, observed)


if __name__ == "__main__":
    unittest.main()

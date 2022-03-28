import unittest
from asyncio import (
    TimeoutError,
    wait_for,
)

from minos.networks import (
    BrokerMessageV1,
    BrokerMessageV1Payload,
)
from minos.plugins.rabbitmq import (
    RabbitMQBrokerPublisher,
    RabbitMQBrokerSubscriber,
)
from tests.utils import (
    CONFIG_FILE_PATH,
)


class IntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_one_topic(self):
        message = BrokerMessageV1("foo", BrokerMessageV1Payload("bar"))

        async with RabbitMQBrokerPublisher.from_config(CONFIG_FILE_PATH) as publisher:
            await publisher.send(message)

        async with RabbitMQBrokerSubscriber.from_config(CONFIG_FILE_PATH, topics={"foo"}) as subscriber:
            observed = await subscriber.receive()

        self.assertEqual(message.content, observed.content)

    async def test_empty_topic(self):
        async with RabbitMQBrokerSubscriber.from_config(CONFIG_FILE_PATH, topics={"empty_topic"}) as subscriber:
            with self.assertRaises(TimeoutError):
                await wait_for(subscriber.receive(), 0.1)


if __name__ == "__main__":
    unittest.main()

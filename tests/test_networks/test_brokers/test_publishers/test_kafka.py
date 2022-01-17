import unittest
from unittest.mock import (
    AsyncMock,
)

from aiokafka import (
    AIOKafkaProducer,
)

from minos.networks import (
    BrokerMessage,
    BrokerMessageV1,
    BrokerMessageV1Payload,
    BrokerPublisher,
    KafkaBrokerPublisher,
)
from tests.utils import (
    CONFIG_FILE_PATH,
)


class TestKafkaBrokerPublisher(unittest.IsolatedAsyncioTestCase):
    def test_is_subclass(self):
        self.assertTrue(issubclass(KafkaBrokerPublisher, BrokerPublisher))

    def test_from_config(self):
        publisher = KafkaBrokerPublisher.from_config(CONFIG_FILE_PATH)
        self.assertIsInstance(publisher, KafkaBrokerPublisher)
        self.assertEqual(publisher.broker_host, "localhost")
        self.assertEqual(publisher.broker_port, 9092)

    async def test_client(self):
        publisher = KafkaBrokerPublisher.from_config(CONFIG_FILE_PATH)

        self.assertIsInstance(publisher.client, AIOKafkaProducer)

    async def test_send(self):
        publisher = KafkaBrokerPublisher.from_config(CONFIG_FILE_PATH)

        send_mock = AsyncMock()
        publisher.client.send_and_wait = send_mock

        message = BrokerMessageV1("foo", BrokerMessageV1Payload("bar"))

        await publisher.send(message)

        self.assertEqual(1, send_mock.call_count)
        self.assertEqual("foo", send_mock.call_args.args[0])
        self.assertEqual(message, BrokerMessage.from_avro_bytes(send_mock.call_args.args[1]))


if __name__ == "__main__":
    unittest.main()

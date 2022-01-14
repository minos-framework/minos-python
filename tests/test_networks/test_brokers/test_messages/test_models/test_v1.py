import unittest
import warnings
from unittest.mock import (
    patch,
)
from uuid import (
    UUID,
    uuid4,
)

from minos.common import (
    Model,
)
from minos.networks import (
    BrokerMessageStrategy,
    BrokerMessageV1,
    BrokerMessageV1Payload,
    BrokerMessageV1Status,
)
from tests.utils import (
    FakeModel,
)


class TestBrokerMessageV1(unittest.TestCase):
    def setUp(self) -> None:
        self.topic = "FooCreated"
        self.identifier = uuid4()
        self.reply_topic = "AddOrderReply"
        self.strategy = BrokerMessageStrategy.MULTICAST

        self.payload = BrokerMessageV1Payload(
            content=[FakeModel("blue"), FakeModel("red")], headers={"foo": "bar"}, status=BrokerMessageV1Status.ERROR
        )

    def test_constructor_simple(self):
        message = BrokerMessageV1(self.topic, self.payload)
        self.assertEqual(self.topic, message.topic)
        self.assertIsInstance(message.identifier, UUID)
        self.assertEqual(None, message.reply_topic)
        self.assertEqual(BrokerMessageStrategy.UNICAST, message.strategy)
        self.assertEqual(self.payload, message.payload)

    def test_constructor(self):
        message = BrokerMessageV1(
            self.topic,
            identifier=self.identifier,
            reply_topic=self.reply_topic,
            strategy=self.strategy,
            payload=self.payload,
        )
        self.assertEqual(self.topic, message.topic)
        self.assertEqual(self.identifier, message.identifier)
        self.assertEqual(self.reply_topic, message.reply_topic)
        self.assertEqual(self.strategy, message.strategy)
        self.assertEqual(self.payload, message.payload)

    def test_version(self):
        self.assertEqual(1, BrokerMessageV1.version)

    def test_topic(self):
        message = BrokerMessageV1(self.topic, self.payload)
        self.assertEqual(self.topic, message.topic)

    def test_identifier(self):
        message = BrokerMessageV1(self.topic, self.payload, identifier=self.identifier)
        self.assertEqual(self.identifier, message.identifier)

    def test_reply_topic(self):
        message = BrokerMessageV1(self.topic, self.payload, reply_topic=self.reply_topic)
        self.assertEqual(self.reply_topic, message.reply_topic)

    def test_ok(self):
        message = BrokerMessageV1(self.topic, self.payload)
        self.assertEqual(self.payload.ok, message.ok)

    def test_status(self):
        message = BrokerMessageV1(self.topic, self.payload)
        self.assertEqual(self.payload.status, message.status)

    def test_headers(self):
        message = BrokerMessageV1(self.topic, self.payload)
        self.assertEqual(self.payload.headers, message.headers)

    def test_content(self):
        message = BrokerMessageV1(self.topic, self.payload)
        self.assertEqual(self.payload.content, message.content)

    def test_data(self):
        message = BrokerMessageV1(self.topic, self.payload)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            # noinspection PyDeprecation
            self.assertEqual(self.payload.content, message.data)

    def test_avro(self):
        message = BrokerMessageV1(
            self.topic,
            identifier=self.identifier,
            reply_topic=self.reply_topic,
            strategy=self.strategy,
            payload=self.payload,
        )
        observed = BrokerMessageV1.from_avro_bytes(message.avro_bytes)
        self.assertEqual(message, observed)

    def test_sort(self):
        unsorted = [
            BrokerMessageV1("", BrokerMessageV1Payload("foo")),
            BrokerMessageV1("", BrokerMessageV1Payload(4)),
            BrokerMessageV1("", BrokerMessageV1Payload(2)),
            BrokerMessageV1("", BrokerMessageV1Payload(3)),
            BrokerMessageV1("", BrokerMessageV1Payload(1)),
            BrokerMessageV1("", BrokerMessageV1Payload("bar")),
        ]

        expected = [unsorted[0], unsorted[4], unsorted[2], unsorted[3], unsorted[1], unsorted[5]]

        observed = sorted(unsorted)
        self.assertEqual(expected, observed)

    def test_from_avro(self):
        expected = BrokerMessageV1(self.topic, self.payload, identifier=self.identifier)
        schema = {
            "fields": [
                {"name": "topic", "type": "string"},
                {"name": "identifier", "type": {"logicalType": "uuid", "type": "string"}},
                {"name": "reply_topic", "type": ["string", "null"]},
                {
                    "name": "strategy",
                    "type": {
                        "logicalType": "minos.networks.brokers.messages.models.v1.BrokerMessageV1Strategy",
                        "type": "string",
                    },
                },
                {
                    "name": "payload",
                    "type": {
                        "fields": [
                            {
                                "name": "content",
                                "type": {
                                    "items": {
                                        "fields": [{"name": "data", "type": "string"}],
                                        "name": "FakeModel",
                                        "namespace": "tests.utils.hello",
                                        "type": "record",
                                    },
                                    "type": "array",
                                },
                            },
                            {
                                "name": "status",
                                "type": {
                                    "logicalType": "minos.networks.brokers.messages.models.v1.BrokerMessageV1Status",
                                    "type": "int",
                                },
                            },
                            {"name": "headers", "type": {"type": "map", "values": "string"}},
                        ],
                        "name": "BrokerMessageV1Payload",
                        "namespace": "minos.networks.brokers.messages.models.v1.hello",
                        "type": "record",
                    },
                },
                {"name": "version", "type": "int"},
            ],
            "name": "BrokerMessage",
            "namespace": "minos.networks.brokers.messages.models.abc.hello",
            "type": "record",
        }
        data = {
            "identifier": str(self.identifier),
            "payload": {"content": [{"data": "blue"}, {"data": "red"}], "headers": {"foo": "bar"}, "status": 400},
            "reply_topic": None,
            "strategy": "unicast",
            "topic": "FooCreated",
            "version": 1,
        }

        observed = Model.from_avro(schema, data)
        self.assertEqual(expected, observed)

    def test_avro_schema(self):
        schema = {
            "fields": [
                {"name": "topic", "type": "string"},
                {"name": "identifier", "type": {"logicalType": "uuid", "type": "string"}},
                {"name": "reply_topic", "type": ["string", "null"]},
                {
                    "name": "strategy",
                    "type": {
                        "logicalType": "minos.networks.brokers.messages.models.v1.BrokerMessageV1Strategy",
                        "type": "string",
                    },
                },
                {
                    "name": "payload",
                    "type": {
                        "fields": [
                            {
                                "name": "content",
                                "type": {
                                    "items": {
                                        "fields": [{"name": "data", "type": "string"}],
                                        "name": "FakeModel",
                                        "namespace": "tests.utils.hello",
                                        "type": "record",
                                    },
                                    "type": "array",
                                },
                            },
                            {
                                "name": "status",
                                "type": {
                                    "logicalType": "minos.networks.brokers.messages.models.v1.BrokerMessageV1Status",
                                    "type": "int",
                                },
                            },
                            {"name": "headers", "type": {"type": "map", "values": "string"}},
                        ],
                        "name": "BrokerMessageV1Payload",
                        "namespace": "minos.networks.brokers.messages.models.v1.hello",
                        "type": "record",
                    },
                },
                {"name": "version", "type": "int"},
            ],
            "name": "BrokerMessage",
            "namespace": "minos.networks.brokers.messages.models.abc.hello",
            "type": "record",
        }
        with patch("minos.common.AvroSchemaEncoder.generate_random_str", return_value="hello"):
            observed = BrokerMessageV1(self.topic, self.payload).avro_schema
        self.assertEqual([schema], observed)

    def test_avro_data(self):
        expected = {
            "identifier": str(self.identifier),
            "payload": {"content": [{"data": "blue"}, {"data": "red"}], "headers": {"foo": "bar"}, "status": 400},
            "reply_topic": None,
            "strategy": "unicast",
            "topic": "FooCreated",
            "version": 1,
        }
        observed = BrokerMessageV1(self.topic, self.payload, identifier=self.identifier).avro_data
        self.assertEqual(expected, observed)

    def test_avro_bytes(self):
        expected = BrokerMessageV1(self.topic, self.payload)
        self.assertEqual(expected, Model.from_avro_bytes(expected.avro_bytes))


class TestBrokerMessagePayload(unittest.TestCase):
    def setUp(self) -> None:
        self.content = [FakeModel("blue"), FakeModel("red")]

    def test_ok(self):
        self.assertTrue(BrokerMessageV1Payload(self.content, status=BrokerMessageV1Status.SUCCESS).ok)
        self.assertFalse(BrokerMessageV1Payload(self.content, status=BrokerMessageV1Status.ERROR).ok)
        self.assertFalse(BrokerMessageV1Payload(self.content, status=BrokerMessageV1Status.SYSTEM_ERROR).ok)
        self.assertFalse(BrokerMessageV1Payload(self.content, status=BrokerMessageV1Status.UNKNOWN).ok)

    def test_data(self):
        payload = BrokerMessageV1Payload(self.content)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            # noinspection PyDeprecation
            self.assertEqual(self.content, payload.data)


class TestBrokerMessageV1Status(unittest.TestCase):
    def test_success(self):
        self.assertEqual(BrokerMessageV1Status.SUCCESS, BrokerMessageV1Status(200))

    def test_error(self):
        self.assertEqual(BrokerMessageV1Status.ERROR, BrokerMessageV1Status(400))

    def test_system_error(self):
        self.assertEqual(BrokerMessageV1Status.SYSTEM_ERROR, BrokerMessageV1Status(500))

    def test_unknown(self):
        self.assertEqual(BrokerMessageV1Status.UNKNOWN, BrokerMessageV1Status(56))


if __name__ == "__main__":
    unittest.main()

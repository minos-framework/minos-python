import unittest
from typing import (
    Any,
    Generic,
    Union,
)
from uuid import (
    UUID,
    uuid4,
)

from minos.aggregate import (
    IS_REPOSITORY_SERIALIZATION_CONTEXT_VAR,
    AggregateRef,
    ModelRef,
)
from minos.common import (
    DeclarativeModel,
    Field,
    Model,
    ModelType,
)
from minos.networks import (
    BrokerMessageV1,
    BrokerMessageV1Payload,
)
from tests.utils import (
    MinosTestCase,
)


class Product(AggregateRef):
    """For testing purposes."""

    title: str
    quantity: int


class TestSubAggregate(unittest.TestCase):
    def test_values(self):
        uuid = uuid4()
        product = Product(uuid, 3, "apple", 3028)

        self.assertEqual(uuid, product.uuid)
        self.assertEqual(3, product.version)
        self.assertEqual("apple", product.title)
        self.assertEqual(3028, product.quantity)


FakeMessage = ModelType.build("FakeMessage", {"content": Any})

Bar = ModelType.build("Bar", {"uuid": UUID, "age": int})
Foo = ModelType.build("Foo", {"another": ModelRef[Bar]})


class TestModelRef(MinosTestCase):
    def test_subclass(self):
        # noinspection PyTypeHints
        self.assertTrue(issubclass(ModelRef, (DeclarativeModel, UUID, Generic)))

    def test_raises(self):
        with self.assertRaises(ValueError):
            # noinspection PyTypeChecker
            ModelRef(56)

    def test_uuid(self):
        uuid = uuid4()
        value = ModelRef(uuid)

        self.assertEqual(uuid, value)

    def test_uuid_int(self):
        uuid = uuid4()
        value = ModelRef(uuid)

        self.assertEqual(uuid.int, value.int)

    def test_uuid_is_safe(self):
        uuid = uuid4()
        value = ModelRef(uuid)

        self.assertEqual(uuid.is_safe, value.is_safe)

    def test_model(self):
        another = Bar(uuid4(), 1)
        value = Foo(another=another)

        self.assertEqual(another, value.another)

    def test_model_uuid(self):
        uuid = uuid4()
        value = ModelRef(Bar(uuid, 1))

        self.assertEqual(uuid, value.uuid)

    def test_model_attribute(self):
        value = ModelRef(Bar(uuid4(), 1))

        self.assertEqual(1, value.age)

    def test_model_attribute_raises(self):
        value = ModelRef(Bar(uuid4(), 1))

        with self.assertRaises(AttributeError):
            value.year

    def test_fields(self):
        value = ModelRef(Bar(uuid4(), 1))

        self.assertEqual({"data": Field("data", Union[Bar, UUID], value)}, value.fields)

    def test_model_avro_data(self):
        value = Bar(uuid4(), 1)

        self.assertEqual(value.avro_data, ModelRef(value).avro_data)

    def test_uuid_avro_data(self):
        value = uuid4()
        self.assertEqual(str(value), ModelRef(value).avro_data)

    async def test_model_avro_data_submitting(self):
        uuid = uuid4()
        value = Bar(uuid, 1)

        IS_REPOSITORY_SERIALIZATION_CONTEXT_VAR.set(True)
        self.assertEqual(str(uuid), ModelRef(value).avro_data)

    async def test_uuid_avro_data_submitting(self):
        value = uuid4()
        IS_REPOSITORY_SERIALIZATION_CONTEXT_VAR.set(True)
        self.assertEqual(str(value), ModelRef(value).avro_data)

    def test_model_avro_schema(self):
        another = Bar(uuid4(), 1)

        expected = [
            [
                {
                    "fields": [
                        {"name": "uuid", "type": {"logicalType": "uuid", "type": "string"}},
                        {"name": "age", "type": "int"},
                    ],
                    "name": "Bar",
                    "namespace": "",
                    "type": "record",
                    "logicalType": "minos.aggregate.models.refs.models.ModelRef",
                },
                {"logicalType": "minos.aggregate.models.refs.models.ModelRef", "type": "string"},
            ]
        ]

        self.assertEqual(expected, ModelRef(another).avro_schema)

    def test_uuid_avro_schema(self):
        another = uuid4()
        ref = Foo(another).another  # FIXME: This should not be needed to set the type hint properly

        expected = [
            [
                {
                    "fields": [
                        {"name": "uuid", "type": {"logicalType": "uuid", "type": "string"}},
                        {"name": "age", "type": "int"},
                    ],
                    "logicalType": "minos.aggregate.models.refs.models.ModelRef",
                    "name": "Bar",
                    "namespace": "",
                    "type": "record",
                },
                {"logicalType": "minos.aggregate.models.refs.models.ModelRef", "type": "string"},
            ]
        ]
        self.assertEqual(expected, ref.avro_schema)

    def test_model_from_avro(self):
        another = Bar(uuid4(), 1)
        expected = Foo(another).another  # FIXME: This should not be needed to set the type hint properly

        schema = [
            {"logicalType": "minos.aggregate.models.refs.models.ModelRef", "type": "string"},
            {
                "fields": [
                    {"name": "uuid", "type": {"logicalType": "uuid", "type": "string"}},
                    {"name": "age", "type": "int"},
                ],
                "logicalType": "minos.aggregate.models.refs.models.ModelRef",
                "name": "Bar",
                "namespace": "",
                "type": "record",
            },
        ]
        data = another.avro_data

        observed = Model.from_avro(schema, data)
        self.assertEqual(expected, observed)

    def test_uuid_from_avro(self):
        another = uuid4()
        expected = Foo(another).another  # FIXME: This should not be needed to set the type hint properly

        schema = [
            {
                "fields": [
                    {"name": "uuid", "type": {"logicalType": "uuid", "type": "string"}},
                    {"name": "age", "type": "int"},
                ],
                "logicalType": "minos.aggregate.models.refs.models.ModelRef",
                "name": "Bar",
                "namespace": "",
                "type": "record",
            },
            {"logicalType": "minos.aggregate.models.refs.models.ModelRef", "type": "string"},
        ]
        data = str(another)

        observed = Model.from_avro(schema, data)
        self.assertEqual(expected, observed)

    async def test_resolve(self):
        another = uuid4()

        self.broker_subscriber_builder.with_messages([BrokerMessageV1("", BrokerMessageV1Payload(Bar(another, 1)))])

        ref = Foo(another).another  # FIXME: This should not be needed to set the type hint properly

        self.assertEqual(ref.data, another)

        await ref.resolve()

        observed = self.broker_publisher.messages
        self.assertEqual(1, len(observed))

        self.assertIsInstance(observed[0], BrokerMessageV1)
        self.assertEqual("GetBar", observed[0].topic)
        self.assertEqual({"uuid": another}, observed[0].content)
        self.assertEqual(ref.data, Bar(another, 1))

    async def test_resolve_already(self):
        uuid = uuid4()

        ref = ModelRef(Bar(uuid, 1))

        await ref.resolve()

        observed = self.broker_publisher.messages
        self.assertEqual(0, len(observed))

    async def test_resolved(self):
        self.assertFalse(ModelRef(uuid4()).resolved)
        self.assertTrue(ModelRef(Bar(uuid4(), 4)).resolved)

    def test_avro_model(self):
        another = Bar(uuid4(), 1)
        ref = Foo(another).another  # FIXME: This should not be needed to set the type hint properly

        self.assertEqual(ref, ModelRef.from_avro_bytes(ref.avro_bytes))

    def test_avro_uuid(self):
        another = uuid4()
        ref = Foo(another).another  # FIXME: This should not be needed to set the type hint properly

        self.assertEqual(ref, ModelRef.from_avro_bytes(ref.avro_bytes))


if __name__ == "__main__":
    unittest.main()

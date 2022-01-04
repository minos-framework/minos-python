import unittest
from typing import (
    Any,
    Generic,
    Union,
)
from unittest.mock import (
    call,
    patch,
)
from uuid import (
    UUID,
    uuid4,
)

from minos.aggregate import (
    IS_REPOSITORY_SERIALIZATION_CONTEXT_VAR,
    AggregateRef,
    FieldRef,
    ModelRef,
)
from minos.common import (
    DeclarativeModel,
    Model,
    ModelType,
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


FakeEntry = ModelType.build("FakeEntry", {"data": Any})
FakeMessage = ModelType.build("FakeMessage", {"data": Any})


class TestModelRef(MinosTestCase):
    def test_subclass(self):
        # noinspection PyTypeHints
        self.assertTrue(issubclass(ModelRef, (DeclarativeModel, UUID, Generic)))

    def test_raises(self):
        with self.assertRaises(ValueError):
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
        # noinspection PyPep8Naming
        Bar = ModelType.build("Bar", {"uuid": UUID, "version": int})
        # noinspection PyPep8Naming
        Foo = ModelType.build("Foo", {"uuid": UUID, "version": int, "another": ModelRef[Bar]})

        another = Bar(uuid=uuid4(), version=1)
        value = Foo(uuid=uuid4(), version=1, another=another)

        self.assertEqual(another, value.another)

    def test_model_uuid(self):
        uuid = uuid4()
        # noinspection PyPep8Naming
        Bar = ModelType.build("Bar", {"uuid": UUID, "version": int})
        value = ModelRef(Bar(uuid=uuid, version=1))

        self.assertEqual(uuid, value.uuid)

    def test_model_attribute(self):
        # noinspection PyPep8Naming
        Bar = ModelType.build("Bar", {"uuid": UUID, "age": int})
        value = ModelRef(Bar(uuid=uuid4(), age=1))

        self.assertEqual(1, value.age)

    def test_model_attribute_raises(self):
        # noinspection PyPep8Naming
        Bar = ModelType.build("Bar", {"uuid": UUID, "age": int})
        value = ModelRef(Bar(uuid=uuid4(), age=1))

        with self.assertRaises(AttributeError):
            value.year

    def test_fields(self):
        # noinspection PyPep8Naming
        Bar = ModelType.build("Bar", {"uuid": UUID, "age": int})
        value = ModelRef(Bar(uuid=uuid4(), age=1))

        self.assertEqual({"data": FieldRef("data", Union[Bar, UUID], value)}, value.fields)

    def test_model_avro_data(self):
        # noinspection PyPep8Naming
        Bar = ModelType.build("Bar", {"uuid": UUID, "age": int})
        value = Bar(uuid=uuid4(), age=1)

        self.assertEqual(value.avro_data, ModelRef(value).avro_data)

    def test_uuid_avro_data(self):
        value = uuid4()
        self.assertEqual(str(value), ModelRef(value).avro_data)

    async def test_model_avro_data_submitting(self):
        # noinspection PyPep8Naming
        Bar = ModelType.build("Bar", {"uuid": UUID, "age": int})
        uuid = uuid4()
        value = Bar(uuid=uuid, age=1)

        IS_REPOSITORY_SERIALIZATION_CONTEXT_VAR.set(True)
        self.assertEqual(str(uuid), ModelRef(value).avro_data)

    async def test_uuid_avro_data_submitting(self):
        value = uuid4()
        IS_REPOSITORY_SERIALIZATION_CONTEXT_VAR.set(True)
        self.assertEqual(str(value), ModelRef(value).avro_data)

    def test_model_avro_schema(self):
        # noinspection PyPep8Naming
        Bar = ModelType.build("Bar", {"uuid": UUID, "age": int})
        value = Bar(uuid=uuid4(), age=1)

        expected = [
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

        self.assertEqual(expected, ModelRef(value).avro_schema)

    def test_uuid_avro_schema(self):
        expected = {
            "logicalType": "minos.aggregate.models.refs.models.ModelRef",
            "type": "string",
        }
        self.assertEqual(expected, ModelRef(uuid4()).avro_schema)

    async def test_resolve(self):
        # noinspection PyPep8Naming
        Bar = ModelType.build("Bar", {"uuid": UUID, "version": int})
        # noinspection PyPep8Naming
        Foo = ModelType.build("Foo", {"another": ModelRef[Bar]})

        uuid = uuid4()

        ref = Foo(uuid).another  # FIXME: This should not be needed to set the type hint properly

        self.assertEqual(ref.data, uuid)

        with patch("tests.utils.FakeBroker.send") as send_mock:
            with patch("tests.utils.FakeBroker.get_one") as get_many:
                get_many.return_value = FakeEntry(FakeMessage(Bar(uuid, 1)))
                await ref.resolve()

        self.assertEqual([call(data={"uuid": uuid}, topic="GetBar")], send_mock.call_args_list)
        self.assertEqual(ref.data, Bar(uuid, 1))

    async def test_resolve_already(self):
        # noinspection PyPep8Naming
        Bar = ModelType.build("Bar", {"uuid": UUID, "version": int})

        uuid = uuid4()

        ref = ModelRef(Bar(uuid, 1))  # FIXME: This should not be needed to set the type hint properly

        with patch("tests.utils.FakeBroker.send") as send_mock:
            await ref.resolve()

        self.assertEqual([], send_mock.call_args_list)

    async def test_resolved(self):
        # noinspection PyPep8Naming
        Bar = ModelType.build("Bar", {"uuid": UUID, "version": int})

        self.assertFalse(ModelRef(uuid4()).resolved)
        self.assertTrue(ModelRef(Bar(uuid4(), 4)).resolved)

    @unittest.skip("Failing test... FIXME!")
    def test_avro_model(self):
        # noinspection PyPep8Naming
        Bar = ModelType.build("Bar", {"uuid": UUID, "version": int})
        base = ModelRef(Bar(uuid4(), 1))
        self.assertEqual(base, ModelRef.from_avro_bytes(base.avro_bytes))

    @unittest.skip("Failing test... FIXME!")
    def test_avro_uuid(self):
        base = ModelRef(uuid4())
        self.assertEqual(base, ModelRef.from_avro_bytes(base.avro_bytes))


if __name__ == "__main__":
    unittest.main()

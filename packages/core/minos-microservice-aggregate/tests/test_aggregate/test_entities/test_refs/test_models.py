import unittest
from typing import (
    Any,
    Generic,
    Union,
)
from unittest.mock import (
    patch,
)
from uuid import (
    UUID,
    uuid4,
)

from minos.aggregate import (
    IS_REPOSITORY_SERIALIZATION_CONTEXT_VAR,
    Ref,
    RefException,
    RefResolver,
)
from minos.common import (
    DeclarativeModel,
    Field,
    Model,
    ModelType,
)
from tests.utils import (
    AggregateTestCase,
)

FakeMessage = ModelType.build("FakeMessage", {"content": Any})

Bar = ModelType.build("Bar", {"uuid": UUID, "age": int})
Foo = ModelType.build("Foo", {"another": Ref[Bar]})


class TestRef(AggregateTestCase):
    def test_subclass(self):
        # noinspection PyTypeHints
        self.assertTrue(issubclass(Ref, (DeclarativeModel, UUID, Generic)))

    def test_raises(self):
        with self.assertRaises(ValueError):
            # noinspection PyTypeChecker
            Ref(56)

    def test_uuid(self):
        uuid = uuid4()
        value = Ref(uuid)

        self.assertEqual(uuid, value)

    def test_uuid_int(self):
        uuid = uuid4()
        value = Ref(uuid)

        self.assertEqual(uuid.int, value.int)

    def test_uuid_is_safe(self):
        uuid = uuid4()
        value = Ref(uuid)

        self.assertEqual(uuid.is_safe, value.is_safe)

    def test_uuid_getattr(self):
        uuid = uuid4()
        value = Ref(uuid)

        self.assertEqual(uuid, value.uuid)

    def test_uuid_getattr_raises(self):
        uuid = uuid4()
        value = Ref(uuid)

        with self.assertRaises(AttributeError):
            value.color

    def test_uuid_setattr(self):
        uuid_1 = uuid4()
        uuid_2 = uuid4()
        value = Ref(uuid_1)

        value.uuid = uuid_2

        self.assertEqual(uuid_2, value.data)

    def test_uuid_getitem(self):
        uuid = uuid4()
        value = Ref(uuid)

        self.assertEqual(uuid, value["uuid"])

    def test_uuid_getitem_raises(self):
        uuid = uuid4()
        value = Ref(uuid)

        with self.assertRaises(KeyError):
            value["color"]

    def test_uuid_setitem(self):
        uuid_1 = uuid4()
        uuid_2 = uuid4()
        value = Ref(uuid_1)

        value["uuid"] = uuid_2

        self.assertEqual(uuid_2, value.data)

    def test_model(self):
        another = Bar(uuid4(), 1)
        value = Foo(another=another)

        self.assertEqual(another, value.another)

    def test_model_uuid(self):
        uuid = uuid4()
        value = Ref(Bar(uuid, 1))

        self.assertEqual(uuid, value.uuid)

    def test_model_getattr(self):
        value = Ref(Bar(uuid4(), 1))

        self.assertEqual(1, value.age)

    def test_model_getattr_raises(self):
        value = Ref(Bar(uuid4(), 1))

        with self.assertRaises(AttributeError):
            value.year

    def test_model_setattr(self):
        value = Ref(Bar(uuid4(), 1))

        value.age = 2

        self.assertEqual(2, value.data.age)

    def test_model_setattr_uuid(self):
        uuid_2 = uuid4()
        value = Ref(Bar(uuid4(), 1))

        value.uuid = uuid_2

        self.assertEqual(uuid_2, value.data)

    def test_model_setattr_raises(self):
        uuid_2 = uuid4()
        value = Ref(Bar(uuid4(), 1))

        with self.assertRaises(AttributeError):
            value.something = uuid_2

    def test_model_getitem(self):
        value = Ref(Bar(uuid4(), 1))

        self.assertEqual(1, value["age"])

    def test_model_getitem_raises(self):
        value = Ref(Bar(uuid4(), 1))

        with self.assertRaises(KeyError):
            value["year"]

    def test_model_setitem(self):
        value = Ref(Bar(uuid4(), 1))

        value["age"] = 2

        self.assertEqual(2, value.data.age)

    def test_model_setitem_uuid(self):
        uuid_2 = uuid4()
        value = Ref(Bar(uuid4(), 1))

        value["uuid"] = uuid_2

        self.assertEqual(uuid_2, value.data)

    def test_model_setitem_raises(self):
        uuid_2 = uuid4()
        value = Ref(Bar(uuid4(), 1))

        with self.assertRaises(KeyError):
            value["something"] = uuid_2

    def test_fields(self):
        value = Ref(Bar(uuid4(), 1))

        self.assertEqual({"data": Field("data", Union[Bar, UUID], value)}, value.fields)

    def test_model_avro_data(self):
        value = Bar(uuid4(), 1)

        self.assertEqual(value.avro_data, Ref(value).avro_data)

    def test_uuid_avro_data(self):
        value = uuid4()
        self.assertEqual(str(value), Ref(value).avro_data)

    async def test_model_avro_data_submitting(self):
        uuid = uuid4()
        value = Bar(uuid, 1)

        IS_REPOSITORY_SERIALIZATION_CONTEXT_VAR.set(True)
        self.assertEqual(str(uuid), Ref(value).avro_data)

    async def test_uuid_avro_data_submitting(self):
        value = uuid4()
        IS_REPOSITORY_SERIALIZATION_CONTEXT_VAR.set(True)
        self.assertEqual(str(value), Ref(value).avro_data)

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
                    "logicalType": "minos.aggregate.entities.refs.models.Ref",
                },
                {"logicalType": "minos.aggregate.entities.refs.models.Ref", "type": "string"},
            ]
        ]

        self.assertEqual(expected, Ref(another).avro_schema)

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
                    "logicalType": "minos.aggregate.entities.refs.models.Ref",
                    "name": "Bar",
                    "namespace": "",
                    "type": "record",
                },
                {"logicalType": "minos.aggregate.entities.refs.models.Ref", "type": "string"},
            ]
        ]
        self.assertEqual(expected, ref.avro_schema)

    def test_model_from_avro(self):
        another = Bar(uuid4(), 1)
        expected = Foo(another).another  # FIXME: This should not be needed to set the type hint properly

        schema = [
            {"logicalType": "minos.aggregate.entities.refs.models.Ref", "type": "string"},
            {
                "fields": [
                    {"name": "uuid", "type": {"logicalType": "uuid", "type": "string"}},
                    {"name": "age", "type": "int"},
                ],
                "logicalType": "minos.aggregate.entities.refs.models.Ref",
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
                "logicalType": "minos.aggregate.entities.refs.models.Ref",
                "name": "Bar",
                "namespace": "",
                "type": "record",
            },
            {"logicalType": "minos.aggregate.entities.refs.models.Ref", "type": "string"},
        ]
        data = str(another)

        observed = Model.from_avro(schema, data)
        self.assertEqual(expected, observed)

    async def test_resolve(self):
        another = uuid4()
        resolved_another = Bar(another, 1)

        ref = Foo(another).another  # FIXME: This should not be needed to set the type hint properly

        self.assertEqual(ref.data, another)

        with patch.object(RefResolver, "resolve", return_value=resolved_another):
            await ref.resolve()

        self.assertEqual(ref.data, Bar(another, 1))

    async def test_resolve_raises(self):
        another = uuid4()

        ref = Foo(another).another  # FIXME: This should not be needed to set the type hint properly

        with self.assertRaises(RefException):
            with patch.object(RefResolver, "resolve", side_effect=RefException("test")):
                await ref.resolve()

    async def test_resolve_already(self):
        uuid = uuid4()

        ref = Ref(Bar(uuid, 1))

        await ref.resolve()

        observed = self.broker_publisher.messages
        self.assertEqual(0, len(observed))

    async def test_resolved(self):
        self.assertFalse(Ref(uuid4()).resolved)
        self.assertTrue(Ref(Bar(uuid4(), 4)).resolved)

    def test_avro_model(self):
        another = Bar(uuid4(), 1)
        ref = Foo(another).another  # FIXME: This should not be needed to set the type hint properly

        self.assertEqual(ref, Ref.from_avro_bytes(ref.avro_bytes))

    def test_avro_uuid(self):
        another = uuid4()
        ref = Foo(another).another  # FIXME: This should not be needed to set the type hint properly

        self.assertEqual(ref, Ref.from_avro_bytes(ref.avro_bytes))

    def test_repr_uuid(self):
        another_uuid = uuid4()
        ref = Foo(another_uuid).another  # FIXME: This should not be needed to set the type hint properly

        self.assertEqual(f"Ref({another_uuid!r})", repr(ref))

    def test_repr_model(self):
        another_uuid = uuid4()
        another = Bar(another_uuid, 1)
        ref = Foo(another).another  # FIXME: This should not be needed to set the type hint properly

        self.assertEqual(f"Ref({another!r})", repr(ref))

    def test_str_uuid(self):
        another_uuid = uuid4()
        ref = Foo(another_uuid).another  # FIXME: This should not be needed to set the type hint properly

        self.assertEqual(str(another_uuid), str(ref))

    def test_str_model(self):
        another_uuid = uuid4()
        another = Bar(another_uuid, 1)
        ref = Foo(another).another  # FIXME: This should not be needed to set the type hint properly

        self.assertEqual(str(another_uuid), str(ref))


if __name__ == "__main__":
    unittest.main()

import unittest
from typing import (
    Generic,
    Optional,
    Union,
)
from uuid import (
    UUID,
    uuid4,
)

from minos.aggregate import (
    SUBMITTING_EVENT_CONTEXT_VAR,
    AggregateRef,
    FieldRef,
    ModelRef,
    ModelRefExtractor,
    ModelRefInjector,
)
from minos.common import (
    DeclarativeModel,
    ModelType,
)
from tests.utils import (
    Car,
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


class TestModelRef(unittest.IsolatedAsyncioTestCase):
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
        mt_bar = ModelType.build("Bar", {"uuid": UUID, "version": int})
        mt_foo = ModelType.build("Foo", {"uuid": UUID, "version": int, "another": ModelRef[mt_bar]})

        another = mt_bar(uuid=uuid4(), version=1)
        value = mt_foo(uuid=uuid4(), version=1, another=another)

        self.assertEqual(another, value.another)

    def test_model_uuid(self):
        uuid = uuid4()
        mt_bar = ModelType.build("Bar", {"uuid": UUID, "version": int})
        value = ModelRef(mt_bar(uuid=uuid, version=1))

        self.assertEqual(uuid, value.uuid)

    def test_model_attribute(self):
        mt_bar = ModelType.build("Bar", {"uuid": UUID, "age": int})
        value = ModelRef(mt_bar(uuid=uuid4(), age=1))

        self.assertEqual(1, value.age)

    def test_model_attribute_raises(self):
        mt_bar = ModelType.build("Bar", {"uuid": UUID, "age": int})
        value = ModelRef(mt_bar(uuid=uuid4(), age=1))

        with self.assertRaises(AttributeError):
            value.year

    def test_fields(self):
        mt_bar = ModelType.build("Bar", {"uuid": UUID, "age": int})
        value = ModelRef(mt_bar(uuid=uuid4(), age=1))

        self.assertEqual({"data": FieldRef("data", Union[mt_bar, UUID], value)}, value.fields)

    def test_model_avro_data(self):
        mt_bar = ModelType.build("Bar", {"uuid": UUID, "age": int})
        value = mt_bar(uuid=uuid4(), age=1)

        self.assertEqual({"data": value.avro_data}, ModelRef(value).avro_data)

    def test_uuid_avro_data(self):
        value = uuid4()
        self.assertEqual({"data": str(value)}, ModelRef(value).avro_data)

    async def test_model_avro_data_submitting(self):
        mt_bar = ModelType.build("Bar", {"uuid": UUID, "age": int})
        uuid = uuid4()
        value = mt_bar(uuid=uuid, age=1)

        SUBMITTING_EVENT_CONTEXT_VAR.set(True)
        self.assertEqual({"data": str(uuid)}, ModelRef(value).avro_data)

    async def test_uuid_avro_data_submitting(self):
        value = uuid4()
        SUBMITTING_EVENT_CONTEXT_VAR.set(True)
        self.assertEqual({"data": str(value)}, ModelRef(value).avro_data)


class TestModelRefExtractor(unittest.TestCase):
    def test_simple(self):
        mt_foo = ModelType.build("Foo", {"uuid": UUID, "version": int})
        value = ModelRef(uuid4())

        expected = {"Foo": {value}}
        observed = ModelRefExtractor(value, ModelRef[mt_foo]).build()

        self.assertEqual(expected, observed)

    def test_list(self):
        mt_foo = ModelType.build("Foo", {"uuid": UUID, "version": int})
        value = [ModelRef(uuid4()), ModelRef(uuid4())]

        expected = {"Foo": set(value)}
        observed = ModelRefExtractor(value, list[ModelRef[mt_foo]]).build()

        self.assertEqual(expected, observed)

    def test_dict(self):
        mt_key = ModelType.build("Key", {"uuid": UUID, "version": int})
        mt_value = ModelType.build("Value", {"uuid": UUID, "version": int})
        value = {ModelRef(uuid4()): ModelRef(uuid4()), ModelRef(uuid4()): ModelRef(uuid4())}

        expected = {"Key": set(value.keys()), "Value": set(value.values())}
        observed = ModelRefExtractor(value, dict[ModelRef[mt_key], ModelRef[mt_value]]).build()

        self.assertEqual(expected, observed)

    def test_model(self):
        mt_bar = ModelType.build("Bar", {"uuid": UUID, "version": int})
        mt_foo = ModelType.build("Foo", {"uuid": UUID, "version": int, "another": ModelRef[mt_bar]})

        value = mt_foo(uuid=uuid4(), version=1, another=ModelRef(uuid4()))

        expected = {"Bar": {value.another}}
        observed = ModelRefExtractor(value, mt_foo).build()

        self.assertEqual(expected, observed)

    def test_model_without_kind(self):
        mt_bar = ModelType.build("Bar", {"uuid": UUID, "version": int})
        mt_foo = ModelType.build("Foo", {"uuid": UUID, "version": int, "another": ModelRef[mt_bar]})

        value = mt_foo(uuid=uuid4(), version=1, another=ModelRef(uuid4()))

        expected = ModelRefExtractor(value, mt_foo).build()
        observed = ModelRefExtractor(value).build()

        self.assertEqual(expected, observed)

    def test_optional(self):
        mt_foo = ModelType.build("Foo", {"uuid": UUID, "version": int})
        value = ModelRef(uuid4())

        expected = {"Foo": {value}}
        observed = ModelRefExtractor(value, Optional[ModelRef[mt_foo]]).build()

        self.assertEqual(expected, observed)

    def test_model_cls_by_type_hints(self):
        mt_foo = ModelType.build("Foo", {"uuid": UUID, "version": int})

        value = ModelRef(mt_foo(uuid4(), 1))
        self.assertEqual(mt_foo, value.data_cls)

    def test_model_cls_none(self):
        value = ModelRef(uuid4())
        self.assertEqual(None, value.data_cls)


class TestModelRefInjector(MinosTestCase):
    async def test_simple(self):
        model = await Car.create(3, "test")
        mapper = {model.uuid: model}

        expected = model
        observed = ModelRefInjector(model.uuid, mapper).build()

        self.assertEqual(expected, observed)

    async def test_list(self):
        model = await Car.create(3, "test")
        mapper = {model.uuid: model}

        expected = [model, model, model]
        observed = ModelRefInjector([model.uuid, model.uuid, model.uuid], mapper).build()

        self.assertEqual(expected, observed)

    async def test_dict(self):
        model = await Car.create(3, "test")
        mapper = {model.uuid: model}

        expected = {model: model}
        observed = ModelRefInjector({model.uuid: model.uuid}, mapper).build()

        self.assertEqual(expected, observed)

    def test_model(self):
        mt_bar = ModelType.build("Bar", {"uuid": UUID, "version": int})
        mt_foo = ModelType.build("Foo", {"uuid": UUID, "version": int, "another": ModelRef[mt_bar]})

        model = mt_bar(uuid=uuid4(), version=1)
        mapper = {model.uuid: model}
        value = mt_foo(uuid=uuid4(), version=1, another=model.uuid)

        expected = mt_foo(uuid=value.uuid, version=1, another=model)
        observed = ModelRefInjector(value, mapper).build()

        self.assertEqual(expected, observed)


if __name__ == "__main__":
    unittest.main()

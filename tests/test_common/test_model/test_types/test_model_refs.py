import unittest
from typing import (
    Generic,
)
from uuid import (
    UUID,
    uuid4,
)

from minos.common import (
    ModelRef,
    ModelRefExtractor,
    ModelRefInjector,
    ModelType,
)
from tests.model_classes import (
    Foo,
)


class TestModelRef(unittest.TestCase):
    def test_subclass(self):
        # noinspection PyTypeHints
        self.assertTrue(issubclass(ModelRef, Generic))


class TestModelRefExtractor(unittest.TestCase):
    def test_simple(self):
        mt_foo = ModelType.build("Foo", {"uuid": UUID, "version": int})
        value = uuid4()

        expected = {"Foo": {value}}
        observed = ModelRefExtractor(value, ModelRef[mt_foo]).build()

        self.assertEqual(expected, observed)

    def test_list(self):
        mt_foo = ModelType.build("Foo", {"uuid": UUID, "version": int})
        value = [uuid4(), uuid4()]

        expected = {"Foo": set(value)}
        observed = ModelRefExtractor(value, list[ModelRef[mt_foo]]).build()

        self.assertEqual(expected, observed)

    def test_dict(self):
        mt_key = ModelType.build("Key", {"uuid": UUID, "version": int})
        mt_value = ModelType.build("Value", {"uuid": UUID, "version": int})
        value = {uuid4(): uuid4(), uuid4(): uuid4()}

        expected = {"Key": set(value.keys()), "Value": set(value.values())}
        observed = ModelRefExtractor(value, dict[ModelRef[mt_key], ModelRef[mt_value]]).build()

        self.assertEqual(expected, observed)

    def test_model(self):
        mt_bar = ModelType.build("Bar", {"uuid": UUID, "version": int})
        mt_foo = ModelType.build("Foo", {"uuid": UUID, "version": int, "another": ModelRef[mt_bar]})

        value = mt_foo(uuid=uuid4(), version=1, another=uuid4())

        expected = {"Bar": {value.another}}
        observed = ModelRefExtractor(value, mt_foo).build()

        self.assertEqual(expected, observed)

    def test_model_without_kind(self):
        mt_bar = ModelType.build("Bar", {"uuid": UUID, "version": int})
        mt_foo = ModelType.build("Foo", {"uuid": UUID, "version": int, "another": ModelRef[mt_bar]})

        value = mt_foo(uuid=uuid4(), version=1, another=uuid4())

        expected = ModelRefExtractor(value, mt_foo).build()
        observed = ModelRefExtractor(value).build()

        self.assertEqual(expected, observed)


class TestModelRefInjector(unittest.TestCase):
    def test_simple(self):
        uuid = uuid4()
        model = Foo("test")
        mapper = {uuid: model}

        expected = model
        observed = ModelRefInjector(uuid, mapper).build()

        self.assertEqual(expected, observed)

    def test_list(self):
        uuid = uuid4()
        model = Foo("test")
        mapper = {uuid: model}

        expected = [model, model, model]
        observed = ModelRefInjector([uuid, uuid, uuid], mapper).build()

        self.assertEqual(expected, observed)

    def test_dict(self):
        uuid = uuid4()
        model = Foo("test")
        mapper = {uuid: model}

        expected = {model: model}
        observed = ModelRefInjector({uuid: uuid}, mapper).build()

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

import unittest
from typing import (
    Optional,
)
from uuid import (
    UUID,
    uuid4,
)

from minos.aggregate import (
    ModelRef,
    ModelRefExtractor,
)
from minos.common import (
    ModelType,
)


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


if __name__ == "__main__":
    unittest.main()

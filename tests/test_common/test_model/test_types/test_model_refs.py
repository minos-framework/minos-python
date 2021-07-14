"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
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
    ModelType,
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


if __name__ == "__main__":
    unittest.main()

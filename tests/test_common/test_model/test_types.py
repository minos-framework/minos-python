"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

import unittest
from typing import (
    Generic,
    TypedDict,
)

from minos.common import (
    DataTransferObject,
    Decimal,
    Enum,
    Fixed,
    MinosModelException,
    MissingSentinel,
    ModelField,
    ModelRef,
    ModelType,
)
from tests.model_classes import (
    Foo,
)


class TestMissingSentinel(unittest.TestCase):
    def test_subclass(self):
        # noinspection PyTypeHints
        self.assertTrue(issubclass(MissingSentinel, Generic))


class TestFixed(unittest.TestCase):
    def test_subclass(self):
        # noinspection PyTypeHints
        self.assertTrue(issubclass(Enum, Generic))

    def test_symbols(self):
        fixed = Fixed(4)
        self.assertEqual(4, fixed.size)

    def test_default(self):
        fixed = Fixed(4)
        self.assertEqual(MissingSentinel, fixed.default)

    def test_namespace(self):
        fixed = Fixed(4)
        self.assertIsNone(fixed.namespace)

    def test_aliases(self):
        fixed = Fixed(4)
        self.assertIsNone(fixed.aliases)

    def test_repr(self):
        fixed = Fixed(4)
        self.assertEqual("Fixed(size=4)", repr(fixed))


class TestEnum(unittest.TestCase):
    def test_subclass(self):
        # noinspection PyTypeHints
        self.assertTrue(issubclass(Enum, Generic))

    def test_symbols(self):
        enum = Enum(["one", "two", "three"])
        self.assertEqual(["one", "two", "three"], enum.symbols)

    def test_default(self):
        enum = Enum(["one", "two", "three"])
        self.assertEqual(MissingSentinel, enum.default)

    def test_namespace(self):
        enum = Enum(["one", "two", "three"])
        self.assertIsNone(enum.namespace)

    def test_aliases(self):
        enum = Enum(["one", "two", "three"])
        self.assertIsNone(enum.aliases)

    def test_docs(self):
        enum = Enum(["one", "two", "three"])
        self.assertIsNone(enum.docs)

    def test_repr(self):
        enum = Enum(["one", "two", "three"])
        self.assertEqual("Enum(symbols=['one', 'two', 'three'])", repr(enum))


class TestDecimal(unittest.TestCase):
    def test_subclass(self):
        # noinspection PyTypeHints
        self.assertTrue(issubclass(Decimal, Generic))

    def test_precision(self):
        decimal = Decimal(6)
        self.assertEqual(6, decimal.precision)

    def test_scale(self):
        decimal = Decimal(6)
        self.assertEqual(0, decimal.scale)

    def test_default(self):
        decimal = Decimal(6)
        self.assertEqual(MissingSentinel, decimal.default)

    def test_aliases(self):
        decimal = Decimal(6)
        self.assertIsNone(decimal.aliases)

    def test_repr(self):
        decimal = Decimal(6)
        self.assertEqual("Decimal(precision=6, scale=0)", repr(decimal))


class TestModelRef(unittest.TestCase):
    def test_subclass(self):
        # noinspection PyTypeHints
        self.assertTrue(issubclass(ModelRef, Generic))

    def test_repr(self):
        ref = ModelRef()
        self.assertEqual("ModelRef()", repr(ref))


class TestModelType(unittest.TestCase):
    def test_build(self):
        model_type = ModelType.build("Foo", {"text": int}, "bar")
        self.assertEqual("Foo", model_type.name)
        self.assertEqual({"text": int}, model_type.type_hints)
        self.assertEqual("bar", model_type.namespace)

    def test_classname(self):
        model_type = ModelType.build("Foo", {"text": int}, "bar")
        self.assertEqual("bar.Foo", model_type.classname)

    def test_hash(self):
        model_type = ModelType.build("Foo", {"text": int}, "bar")
        self.assertIsInstance(hash(model_type), int)

    def test_equal(self):
        one = ModelType.build("Foo", {"text": int}, "bar")
        two = ModelType.build("Foo", {"text": int}, "bar")
        self.assertEqual(one, two)

    def test_equal_declarative(self):
        one = ModelType.build("tests.model_classes.Foo", {"text": str})
        self.assertEqual(one, Foo)

    def test_not_equal(self):
        base = ModelType.build("Foo", {"text": int}, "bar")
        self.assertNotEqual(ModelType.build("aaa", {"text": int}, "bar"), base)
        self.assertNotEqual(ModelType.build("Foo", {"aaa": float}, "bar"), base)
        self.assertNotEqual(ModelType.build("Foo", {"text": int}, "aaa"), base)

    def test_from_typed_dict(self):
        expected = ModelType.build("Foo", {"text": int}, "bar")
        observed = ModelType.from_typed_dict(TypedDict("bar.Foo", {"text": int}))
        self.assertEqual(expected, observed)

    def test_from_typed_dict_without_namespace(self):
        expected = ModelType.build("Foo", {"text": int})
        observed = ModelType.from_typed_dict(TypedDict("Foo", {"text": int}))
        self.assertEqual(expected, observed)

    def test_call_declarative_model(self):
        model_type = ModelType.build("tests.model_classes.Foo", {"text": str})
        dto = model_type(text="test")
        self.assertEqual(Foo("test"), dto)

    def test_call_declarative_model_raises(self):
        model_type = ModelType.build("tests.model_classes.Foo", {"bar": str})
        with self.assertRaises(MinosModelException):
            model_type(bar="test")

    def test_call_dto_model(self):
        model_type = ModelType.build("Foo", {"text": str})
        dto = model_type(text="test")
        self.assertEqual(DataTransferObject("Foo", [ModelField("text", str, "test")]), dto)


if __name__ == "__main__":
    unittest.main()

"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

import unittest
from typing import (
    Generic,
)

from minos.common import (
    CUSTOM_TYPES,
    Decimal,
    Enum,
    Fixed,
    MissingSentinel,
    ModelRef,
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


class TestTypesModule(unittest.TestCase):
    def test_custom_types(self):
        self.assertEqual(("Fixed", "Enum", "Decimal", "ModelRef",), CUSTOM_TYPES)


if __name__ == "__main__":
    unittest.main()

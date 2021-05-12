"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

import unittest

from minos.saga import (
    SagaContext,
)
from tests.utils import (
    Foo,
)


class TestSagaContext(unittest.TestCase):
    def test_constructor(self):
        context = SagaContext(one=1, two="two", three=Foo("three"))
        self.assertEqual(1, context.one)
        self.assertEqual("two", context.two)
        self.assertEqual(Foo("three"), context.three)

    def test_constructor_types(self):
        context = SagaContext(one=1, two="two", three=Foo("three"))
        self.assertEqual({"one": "builtins.int", "two": "builtins.str", "three": "tests.utils.Foo"}, context.types_)

    def test_setter(self):
        context = SagaContext()
        context.one = 1
        context.two = "two"
        context.three = Foo("three")
        self.assertEqual(1, context.one)
        self.assertEqual("two", context.two)
        self.assertEqual(Foo("three"), context.three)

    def test_setter_types(self):
        context = SagaContext()
        context.one = 1
        context.two = "two"
        context.three = Foo("three")
        self.assertEqual({"one": "builtins.int", "two": "builtins.str", "three": "tests.utils.Foo"}, context.types_)

    def test_avro(self):
        original = SagaContext(one=1, two="two", three=Foo("three"))
        another = SagaContext.from_avro_bytes(original.avro_bytes)
        self.assertEqual(original, another)


if __name__ == "__main__":
    unittest.main()

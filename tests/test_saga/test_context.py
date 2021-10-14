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

    def test_setter(self):
        context = SagaContext()
        context.one = 1
        context.two = "two"
        context.three = Foo("three")
        self.assertEqual(1, context.one)
        self.assertEqual("two", context.two)
        self.assertEqual(Foo("three"), context.three)

    def test_item_setter(self):
        context = SagaContext()
        context["one"] = 1
        context["two"] = "two"
        context["three"] = Foo("three")
        self.assertEqual(SagaContext(one=1, two="two", three=Foo("three")), context)

    def test_item_getter(self):
        context = SagaContext(one=1, two="two", three=Foo("three"))
        self.assertEqual(1, context["one"])
        self.assertEqual("two", context["two"])
        self.assertEqual(Foo("three"), context["three"])

    def test_avro(self):
        original = SagaContext(one=1, two="two", three=Foo("three"))
        another = SagaContext.from_avro_bytes(original.avro_bytes)
        self.assertEqual(original, another)

    def test_change_type(self):
        context = SagaContext(one=1)
        context["one"] = "one"
        self.assertEqual(SagaContext(one="one"), context)


if __name__ == "__main__":
    unittest.main()

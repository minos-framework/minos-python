import unittest
from typing import (
    TypedDict,
)

from minos.common import (
    DataTransferObject,
    Field,
    MinosReqAttributeException,
    ModelType,
)
from tests.model_classes import (
    Foo,
)


class TestModelType(unittest.TestCase):
    def test_build(self):
        model_type = ModelType.build("Foo", {"text": int})
        self.assertEqual("Foo", model_type.name)
        self.assertEqual({"text": int}, model_type.type_hints)
        self.assertEqual(str(), model_type.namespace)

    def test_build_with_kwargs(self):
        model_type = ModelType.build("Foo", text=int)
        self.assertEqual("Foo", model_type.name)
        self.assertEqual({"text": int}, model_type.type_hints)
        self.assertEqual(str(), model_type.namespace)

    def test_build_with_namespace(self):
        model_type = ModelType.build("Foo", {"text": int}, namespace_="bar")
        self.assertEqual("Foo", model_type.name)
        self.assertEqual({"text": int}, model_type.type_hints)
        self.assertEqual("bar", model_type.namespace)

    def test_build_raises(self):
        with self.assertRaises(ValueError):
            ModelType.build("Foo", {"text": int}, foo=int)

    def test_classname(self):
        model_type = ModelType.build("Foo", {"text": int}, namespace_="bar")
        self.assertEqual("bar.Foo", model_type.classname)

    def test_hash(self):
        model_type = ModelType.build("Foo", {"text": int}, namespace_="bar")
        self.assertIsInstance(hash(model_type), int)

    def test_lt(self):
        one = ModelType.build("Foo", {"text": int}, namespace_="bar")
        two = ModelType.build("Foo", {"text": int, "number": int}, namespace_="bar")
        self.assertLess(one, two)

    def test_le(self):
        one = ModelType.build("Foo", {"text": int}, namespace_="bar")
        two = ModelType.build("Foo", {"text": int, "number": int}, namespace_="bar")
        three = ModelType.build("Foo", {"text": int, "number": int}, namespace_="bar")
        self.assertLessEqual(one, two)
        self.assertLessEqual(two, three)

    def test_gt(self):
        one = ModelType.build("Foo", {"text": int, "number": int}, namespace_="bar")
        two = ModelType.build("Foo", {"text": int}, namespace_="bar")
        self.assertGreater(one, two)

    def test_ge(self):
        one = ModelType.build("Foo", {"text": int, "number": int}, namespace_="bar")
        two = ModelType.build("Foo", {"text": int, "number": int}, namespace_="bar")
        three = ModelType.build("Foo", {"text": int}, namespace_="bar")
        self.assertGreaterEqual(one, two)
        self.assertGreaterEqual(two, three)

    def test_equal(self):
        one = ModelType.build("Foo", {"text": int}, namespace_="bar")
        two = ModelType.build("Foo", {"text": int}, namespace_="bar")
        self.assertEqual(one, two)

    def test_equal_declarative(self):
        one = ModelType.build("tests.model_classes.Foo", {"text": str})
        self.assertEqual(one, Foo)

    def test_not_equal(self):
        base = ModelType.build("Foo", {"text": int}, namespace_="bar")
        self.assertNotEqual(ModelType.build("aaa", {"text": int}, namespace_="bar"), base)
        self.assertNotEqual(ModelType.build("Foo", {"aaa": float}, namespace_="bar"), base)
        self.assertNotEqual(ModelType.build("Foo", {"text": int}, namespace_="aaa"), base)

    def test_from_typed_dict(self):
        expected = ModelType.build("Foo", {"text": int}, namespace_="bar")
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
        with self.assertRaises(MinosReqAttributeException):
            model_type(bar="test")

    def test_call_dto_model(self):
        model_type = ModelType.build("Foo", {"text": str})
        dto = model_type(text="test")
        self.assertEqual(DataTransferObject("Foo", [Field("text", str, "test")]), dto)


if __name__ == "__main__":
    unittest.main()

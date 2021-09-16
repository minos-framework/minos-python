import unittest
from typing import (
    Any,
    Optional,
    Union,
)
from uuid import (
    UUID,
)

from minos.common import (
    Aggregate,
    ModelRef,
    ModelType,
    TypeHintComparator,
)
from tests.aggregate_classes import (
    Car,
    Owner,
)
from tests.model_classes import (
    Foo,
)


class _A:
    """For testing purposes."""


class _B(_A):
    """For testing purposes."""


class TestTypeHintComparator(unittest.TestCase):
    def test_immutable_true(self):
        self.assertTrue(TypeHintComparator(int, int).match())

    def test_immutable_false(self):
        self.assertFalse(TypeHintComparator(int, float).match())

    def test_optional_true(self):
        self.assertTrue(TypeHintComparator(Optional[int], Optional[int]).match())

    def test_optional_union(self):
        self.assertTrue(TypeHintComparator(Optional[str], Union[str, None]).match())

    def test_optional_false(self):
        self.assertFalse(TypeHintComparator(Optional[int], Optional[float]).match())

    def test_list_true(self):
        self.assertTrue(TypeHintComparator(list[int], list[int]).match())

    def test_list_false(self):
        self.assertFalse(TypeHintComparator(list[int], list[float]).match())
        self.assertFalse(TypeHintComparator(list[int], list).match())

    def test_dict_true(self):
        self.assertTrue(TypeHintComparator(dict[str, int], dict[str, int]).match())

    def test_dict_false(self):
        self.assertFalse(TypeHintComparator(dict[str, int], dict[str, float]).match())
        self.assertFalse(TypeHintComparator(dict[str, int], dict[int, int]).match())
        self.assertFalse(TypeHintComparator(dict[str, int], dict).match())
        self.assertFalse(TypeHintComparator(dict[str, int], dict[str]).match())

    def test_inherited(self):
        self.assertTrue(TypeHintComparator(_B, _A).match())
        self.assertFalse(TypeHintComparator(_A, _B).match())

    def test_any(self):
        self.assertTrue(TypeHintComparator(int, Any).match())
        self.assertFalse(TypeHintComparator(Any, int).match())

    def test_model_ref_union(self):
        self.assertTrue(TypeHintComparator(ModelRef[str], Union[str, UUID]).match())

    def test_nested_true(self):
        self.assertTrue(TypeHintComparator(Optional[list[ModelRef[str]]], Optional[list[ModelRef[str]]]).match())

    def test_nested_false(self):
        self.assertFalse(TypeHintComparator(Optional[list[ModelRef[str]]], Optional[list[ModelRef[float]]]).match())

    def test_model_true(self):
        self.assertTrue(TypeHintComparator(Car, Car).match())

    # noinspection PyTypeChecker
    def test_model_type_true(self):
        self.assertTrue(TypeHintComparator(Car.model_type, Car.model_type).match())
        self.assertTrue(TypeHintComparator(Car, Car.model_type).match())
        self.assertTrue(TypeHintComparator(Car.model_type, Car).match())

    def test_model_false(self):
        self.assertFalse(TypeHintComparator(Car, Owner).match())

    def test_model_type_nested_true(self):
        self.assertTrue(TypeHintComparator(list[Car.model_type], list[Car.model_type]).match())
        self.assertTrue(TypeHintComparator(list[Car], list[Car.model_type]).match())
        self.assertTrue(TypeHintComparator(list[Car.model_type], list[Car]).match())

    def test_model_nested_false(self):
        self.assertFalse(TypeHintComparator(list[Car], list[Owner]).match())

    def test_model_inherited(self):
        self.assertTrue(TypeHintComparator(list[Car], list[Aggregate]).match())
        self.assertFalse(TypeHintComparator(list[Aggregate], list[Car]).match())

        self.assertTrue(TypeHintComparator(list[Car.model_type], list[Aggregate]).match())
        self.assertTrue(TypeHintComparator(list[Aggregate], list[Car.model_type]).match())

    def test_model_type_inequality_true(self):
        one = ModelType.build("Foo", {"text": int}, namespace_="bar")
        two = ModelType.build("Foo", {"text": int, "number": int}, namespace_="bar")
        self.assertTrue(TypeHintComparator(one, two).match())

    def test_model_type_inequality_false(self):
        one = ModelType.build("Foo", {"text": int, "number": int}, namespace_="bar")
        two = ModelType.build("Foo", {"text": int}, namespace_="bar")
        self.assertFalse(TypeHintComparator(one, two).match())

    def test_equal_optional(self):
        one = ModelType.build("Foo", {"text": int}, namespace_="bar")
        two = ModelType.build("Foo", {"text": int}, namespace_="bar")
        self.assertTrue(TypeHintComparator(Optional[one], Optional[two]).match())

    def test_equal_optional_false(self):
        one = ModelType.build("Foo", {"text": int}, namespace_="bar")
        two = ModelType.build("Foo", {"text": float}, namespace_="bar")
        self.assertFalse(TypeHintComparator(Optional[one], Optional[two]).match())

    def test_equal_declarative(self):
        one = ModelType.build("tests.model_classes.Foo", {"text": str})
        self.assertTrue(TypeHintComparator(one, Foo).match())

    def test_equal_declarative_false(self):
        one = ModelType.build("tests.model_classes.Foo", {"text": float})
        self.assertFalse(TypeHintComparator(one, Foo).match())

    def test_equal_declarative_optional(self):
        one = ModelType.build("tests.model_classes.Foo", {"text": int})
        two = ModelType.build("tests.model_classes.Foo", {"text": int})
        self.assertTrue(TypeHintComparator(Optional[one], Optional[two]).match())

    def test_equal_declarative_optional_false(self):
        one = ModelType.build("tests.model_classes.Foo", {"text": int})
        two = ModelType.build("tests.model_classes.Foo", {"text": float})
        self.assertFalse(TypeHintComparator(Optional[one], Optional[two]).match())


if __name__ == "__main__":
    unittest.main()

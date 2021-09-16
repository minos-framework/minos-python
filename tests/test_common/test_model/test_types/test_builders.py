import unittest
from typing import (
    Any,
    Union,
)
from uuid import (
    uuid4,
)

from minos.common import (
    ModelRef,
    ModelType,
    TypeHintBuilder,
)
from tests.model_classes import (
    Foo,
)
from tests.subaggregate_classes import (
    CartItem,
)


class TestTypeHintBuilder(unittest.TestCase):
    def test_immutable(self):
        self.assertEqual(int, TypeHintBuilder(34).build())

    def test_list(self):
        self.assertEqual(list[Union[int, str]], TypeHintBuilder([34, "hello", 12]).build())

    def test_list_empty(self):
        self.assertEqual(list[Any], TypeHintBuilder([]).build())

    def test_list_empty_with_base(self):
        self.assertEqual(list[int], TypeHintBuilder([], list[int]).build())

    def test_dict(self):
        self.assertEqual(dict[str, int], TypeHintBuilder({"one": 1, "two": 2}).build())

    def test_dict_empty(self):
        self.assertEqual(dict[Any, Any], TypeHintBuilder(dict()).build())

    def test_dict_empty_with_base(self):
        self.assertEqual(dict[str, float], TypeHintBuilder(dict(), dict[str, float]).build())

    def test_model_type(self):
        one = ModelType.build("tests.model_classes.Foo", {"text": str})
        v = [Foo("hello"), one(text="bye")]
        self.assertEqual(list[one], TypeHintBuilder(v).build())

    def test_model_ref(self):
        expected = list[ModelRef[CartItem]]
        observed = TypeHintBuilder([uuid4(), uuid4()], list[ModelRef[CartItem]]).build()
        self.assertEqual(expected, observed)

    def test_union_any(self):
        expected = list[int]
        observed = TypeHintBuilder([123], list[Union[int, Any]]).build()
        self.assertEqual(expected, observed)


if __name__ == "__main__":
    unittest.main()

import unittest
from typing import (
    Any,
    Optional,
    Union,
)

from minos.common import (
    ModelType,
    TypeHintBuilder,
    TypeHintParser,
)
from tests.model_classes import (
    Foo,
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
        self.assertEqual(dict[str, Any], TypeHintBuilder(dict()).build())

    def test_dict_empty_with_base(self):
        self.assertEqual(dict[str, float], TypeHintBuilder(dict(), dict[str, float]).build())

    def test_model_type(self):
        one = ModelType.build("tests.model_classes.Foo", {"text": str})
        v = [Foo("hello"), one(text="bye")]
        self.assertEqual(list[one], TypeHintBuilder(v).build())

    def test_union_any(self):
        expected = list[int]
        observed = TypeHintBuilder([123], list[Union[int, Any]]).build()
        self.assertEqual(expected, observed)


class TestTypeHintParser(unittest.TestCase):
    def test_immutable(self):
        self.assertEqual(int, TypeHintParser(int).build())

    def test_optional(self):
        self.assertEqual(Optional[int], TypeHintParser(Optional[int]).build())

    def test_model(self):
        # noinspection PyPep8Naming
        FooMt = ModelType.build("tests.model_classes.Foo", {"text": str})
        self.assertEqual(FooMt, TypeHintParser(Foo).build())

    def test_nested_model(self):
        # noinspection PyPep8Naming
        FooMt = ModelType.build("tests.model_classes.Foo", {"text": str})
        self.assertEqual(Optional[FooMt], TypeHintParser(Optional[Foo]).build())

    def test_model_type(self):
        # noinspection PyPep8Naming
        FooMt = ModelType.build("tests.model_classes.Foo", {"text": str})
        self.assertEqual(FooMt, TypeHintParser(FooMt).build())

    # noinspection PyPep8Naming
    def test_model_type_nested(self):
        Base = ModelType.build("Base", {"another": Foo})
        FooMt = ModelType.build("tests.model_classes.Foo", {"text": str})
        Expected = ModelType.build("Base", {"another": FooMt})
        self.assertEqual(Expected, TypeHintParser(Base).build())


if __name__ == "__main__":
    unittest.main()

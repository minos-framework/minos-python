"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest
from typing import (
    Union,
)

from minos.common import (
    ModelType,
    TypeHintBuilder,
)
from tests.model_classes import (
    Foo,
)


class TestTypeHintBuilder(unittest.TestCase):
    def test_immutable(self):
        self.assertEqual(int, TypeHintBuilder(34).build())

    def test_list(self):
        self.assertEqual(list[Union[int, str]], TypeHintBuilder([34, "hello"]).build())

    def test_model_type(self):
        one = ModelType.build("tests.model_classes.Foo", {"text": str})
        v = [Foo("hello"), one(text="bye")]
        self.assertEqual(list[one], TypeHintBuilder(v).build())


if __name__ == "__main__":
    unittest.main()

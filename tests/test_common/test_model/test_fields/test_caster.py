"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

import unittest
from typing import (
    TypedDict,
)

from minos.common import (
    DataTransferObject,
    MinosTypeAttributeException,
    ModelField,
    ModelFieldCaster,
)


class TestModelFieldCaster(unittest.TestCase):
    def test_typed_dict(self):
        observed = ModelFieldCaster("test", TypedDict("Foo", {"bar": str})).cast({"bar": "foobar"})

        self.assertIsInstance(observed, DataTransferObject)
        self.assertEqual({"bar": "foobar"}, observed.avro_data)

    def test_typed_dict_already_casted(self):
        value = DataTransferObject("Foo", fields={"bar": ModelField("bar", str, "foobar")})
        observed = ModelFieldCaster("test", TypedDict("Foo", {"bar": str})).cast(value)
        self.assertEqual(value, observed)

    def test_typed_dict_raises(self):
        with self.assertRaises(MinosTypeAttributeException):
            ModelFieldCaster("test", TypedDict("Foo", {"bar": str})).cast(3)


if __name__ == "__main__":
    unittest.main()

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
    AvroDataDecoder,
    DataTransferObject,
    MinosTypeAttributeException,
    ModelField,
)


class TestAvroDataDecoder(unittest.TestCase):
    def test_typed_dict(self):
        observed = AvroDataDecoder("test", TypedDict("Foo", {"bar": str})).build({"bar": "foobar"})

        self.assertIsInstance(observed, DataTransferObject)
        self.assertEqual({"bar": "foobar"}, observed.avro_data)

    def test_typed_dict_already_casted(self):
        value = DataTransferObject("Foo", fields={"bar": ModelField("bar", str, "foobar")})
        observed = AvroDataDecoder("test", TypedDict("Foo", {"bar": str})).build(value)
        self.assertEqual(value, observed)

    def test_typed_dict_raises(self):
        with self.assertRaises(MinosTypeAttributeException):
            AvroDataDecoder("test", TypedDict("Foo", {"bar": str})).build(3)


if __name__ == "__main__":
    unittest.main()

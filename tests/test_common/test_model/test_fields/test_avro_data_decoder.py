"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

import unittest

from minos.common import (
    AvroDataDecoder,
    DataTransferObject,
    MinosTypeAttributeException,
    ModelField,
    ModelType,
)


class TestAvroDataDecoder(unittest.TestCase):
    def test_model_typ(self):
        observed = AvroDataDecoder("test", ModelType.build("Foo", {"bar": str})).build({"bar": "foobar"})

        self.assertIsInstance(observed, DataTransferObject)
        self.assertEqual({"bar": "foobar"}, observed.avro_data)

    def test_model_type_already_casted(self):
        value = DataTransferObject("Foo", fields={"bar": ModelField("bar", str, "foobar")})
        observed = AvroDataDecoder("test", ModelType.build("Foo", {"bar": str})).build(value)
        self.assertEqual(value, observed)

    def test_model_type_raises(self):
        with self.assertRaises(MinosTypeAttributeException):
            AvroDataDecoder("test", ModelType.build("Foo", {"bar": str})).build(3)


if __name__ == "__main__":
    unittest.main()

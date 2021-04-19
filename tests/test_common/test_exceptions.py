"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest

from minos.common import (
    EmptyMinosModelSequenceException,
    MinosAttributeValidationException,
    MinosException,
    MinosMalformedAttributeException,
    MinosModelAttributeException,
    MinosModelException,
    MinosParseAttributeException,
    MinosReqAttributeException,
    MinosTypeAttributeException,
    MultiTypeMinosModelSequenceException,
)


class TestExceptions(unittest.TestCase):
    def test_type(self):
        self.assertTrue(issubclass(MinosException, Exception))

    def test_base_repr(self):
        exception = MinosException("test")
        self.assertEqual("MinosException(message='test')", repr(exception))

    def test_base_str(self):
        exception = MinosException("test")
        self.assertEqual("test", str(exception))

    def test_model(self):
        self.assertTrue(issubclass(MinosModelException, MinosException))

    def test_model_emtpy_sequence(self):
        self.assertTrue(issubclass(EmptyMinosModelSequenceException, MinosModelException))

    def test_model_multi_type_sequence(self):
        self.assertTrue(issubclass(MultiTypeMinosModelSequenceException, MinosModelException))

    def test_model_attribute(self):
        self.assertTrue(issubclass(MinosModelAttributeException, MinosException))

    def test_required_attribute(self):
        self.assertTrue(issubclass(MinosReqAttributeException, MinosModelAttributeException))

    def test_type_attribute(self):
        self.assertTrue(issubclass(MinosTypeAttributeException, MinosModelAttributeException))

    def test_type_attribute_repr(self):
        exception = MinosTypeAttributeException("foo", float, True)
        message = (
            "MinosTypeAttributeException(message=\"The <class 'float'> expected type for 'foo' "
            "does not match with the given data type: <class 'bool'>\")"
        )
        self.assertEqual(message, repr(exception))

    def test_malformed_attribute(self):
        self.assertTrue(issubclass(MinosMalformedAttributeException, MinosModelAttributeException))

    def test_parse_attribute(self):
        self.assertTrue(issubclass(MinosParseAttributeException, MinosModelAttributeException))

    def test_attribute_parse_repr(self):
        exception = MinosParseAttributeException("foo", 34, ValueError())
        message = (
            'MinosParseAttributeException(message="ValueError() '
            "was raised while parsing 'foo' field with 34 value.\")"
        )
        self.assertEqual(message, repr(exception))

    def test_attribute_validation(self):
        self.assertTrue(issubclass(MinosAttributeValidationException, MinosModelAttributeException))

    def test_attribute_validation_repr(self):
        exception = MinosAttributeValidationException("foo", 34)
        message = "MinosAttributeValidationException(message=\"34 value does not pass the 'foo' field validation.\")"
        self.assertEqual(message, repr(exception))


if __name__ == "__main__":
    unittest.main()

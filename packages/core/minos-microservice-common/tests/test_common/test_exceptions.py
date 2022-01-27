import unittest

from minos.common import (
    EmptyMinosModelSequenceException,
    MinosAttributeValidationException,
    MinosBrokerException,
    MinosConfigException,
    MinosException,
    MinosHandlerException,
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

    def test_config(self):
        self.assertTrue(issubclass(MinosConfigException, MinosException))

    def test_broker(self):
        self.assertTrue(issubclass(MinosBrokerException, MinosException))

    def test_handler(self):
        self.assertTrue(issubclass(MinosHandlerException, MinosException))

    def test_model(self):
        self.assertTrue(issubclass(MinosModelException, MinosException))

    def test_model_empty_sequence(self):
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
            "does not match with the given data type: <class 'bool'> (True)\")"
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

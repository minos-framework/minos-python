"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest

from minos.common import (
    EmptyMinosModelSequenceException,
    MinosAttributeValidationException,
    MinosConfigDefaultAlreadySetException,
    MinosConfigException,
    MinosException,
    MinosMalformedAttributeException,
    MinosModelAttributeException,
    MinosModelException,
    MinosParseAttributeException,
    MinosRepositoryAggregateNotFoundException,
    MinosRepositoryDeletedAggregateException,
    MinosRepositoryException,
    MinosRepositoryManuallySetAggregateIdException,
    MinosRepositoryManuallySetAggregateVersionException,
    MinosRepositoryNonProvidedException,
    MinosRepositoryUnknownActionException,
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

    def test_config_default_already_set(self):
        self.assertTrue(issubclass(MinosConfigDefaultAlreadySetException, MinosConfigException))

    def test_repository_aggregate_not_found(self):
        self.assertTrue(issubclass(MinosRepositoryAggregateNotFoundException, MinosRepositoryException))

    def test_repository_deleted_aggregate(self):
        self.assertTrue(issubclass(MinosRepositoryDeletedAggregateException, MinosRepositoryException))

    def test_repository_manually_set_aggregate_id(self):
        self.assertTrue(issubclass(MinosRepositoryManuallySetAggregateIdException, MinosRepositoryException))

    def test_repository_manually_set_aggregate_version(self):
        self.assertTrue(issubclass(MinosRepositoryManuallySetAggregateVersionException, MinosRepositoryException))

    def test_repository_bad_action(self):
        self.assertTrue(issubclass(MinosRepositoryUnknownActionException, MinosRepositoryException))

    def test_repository_non_set(self):
        self.assertTrue(issubclass(MinosRepositoryNonProvidedException, MinosRepositoryException))

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

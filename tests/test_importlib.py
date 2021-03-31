import pytest

from minos.common.importlib import import_module


def test_import_module():
    object_class = import_module("tests.ImportedModule.ImportedClassTest")
    instance_class = object_class()
    assert instance_class.return_test_example() == "test passed"

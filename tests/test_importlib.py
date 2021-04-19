"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

import pytest
from minos.common import (
    MinosImportException,
    import_module,
)


def test_import_module():
    object_class = import_module("tests.ImportedModule.ImportedClassTest")
    instance_class = object_class()
    assert instance_class.return_test_example() == "test passed"


def test_import_module_exception():
    with pytest.raises(MinosImportException):
        object_class = import_module("tests.ImportedModuleFail.ImportedClassTest")
        assert True == True

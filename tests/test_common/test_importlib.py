"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

import unittest
from minos.common import (
    MinosImportException,
    import_module,
    classname,
)


class TestImportlib(unittest.TestCase):

    def test_import_module(self):
        object_class = import_module("tests.ImportedModule.ImportedClassTest")
        self.assertEqual("tests.ImportedModule.ImportedClassTest", classname(object_class))

    def test_import_module_exception(self):
        with self.assertRaises(MinosImportException):
            import_module("tests.ImportedModuleFail.ImportedClassTest")

    def test_classname(self):
        self.assertEqual("builtins.int", classname(int))
        self.assertEqual("minos.common.exceptions.MinosImportException", classname(MinosImportException))


if __name__ == '__main__':
    unittest.main()

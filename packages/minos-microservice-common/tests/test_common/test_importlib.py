import unittest

from minos.common import (
    MinosImportException,
    classname,
    import_module,
)


class TestImportlib(unittest.TestCase):
    def test_import_module_first_level(self):
        module = import_module("functools")
        self.assertEqual("functools", classname(module))

    def test_import_module(self):
        object_class = import_module("tests.ImportedModule.ImportedClassTest")
        self.assertEqual("tests.ImportedModule.ImportedClassTest", classname(object_class))

    def test_import_module_raises(self):
        with self.assertRaises(MinosImportException):
            import_module("tests.ImportedModuleFail.ImportedClassTest")

        with self.assertRaises(MinosImportException):
            import_module("builtins.foo")

    def test_classname(self):
        self.assertEqual("builtins.int", classname(int))
        self.assertEqual("minos.common.exceptions.MinosImportException", classname(MinosImportException))


if __name__ == "__main__":
    unittest.main()

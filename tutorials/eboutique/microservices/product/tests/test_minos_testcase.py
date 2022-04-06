import unittest
from unittest.mock import patch

from .minos_testcase import MinosTestCase


class TestMinosTestCase(unittest.TestCase):
    def test_something(self):
        self.assertEqual(True, True)

    # @patch("pytest_kind.KindCluster")
    def test_load_services(self):
        test_case = MinosTestCase()
        test_case.setUp()

        test_case.load_services()


if __name__ == '__main__':
    unittest.main()

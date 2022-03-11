import unittest

from minos.plugins.graphql import (
    GraphQlEnroute,
)


class TestSomething(unittest.TestCase):

    def test_true(self):
        self.assertTrue(GraphQlEnroute)


if __name__ == '__main__':
    unittest.main()

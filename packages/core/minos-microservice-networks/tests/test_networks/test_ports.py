import unittest

from aiomisc import (
    Service,
)

from minos.networks import (
    Port,
)


class TestPort(unittest.TestCase):
    def test_is_subclass(self):
        self.assertTrue(issubclass(Port, Service))

    def test_abstract(self):
        # noinspection PyUnresolvedReferences
        self.assertEqual({"start", "stop"}, Port.__abstractmethods__)


if __name__ == "__main__":
    unittest.main()

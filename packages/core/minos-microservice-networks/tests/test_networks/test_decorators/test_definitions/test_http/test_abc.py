import unittest

from minos.networks import (
    EnrouteDecorator,
    HttpEnrouteDecorator,
)


class _HttpEnrouteDecorator(HttpEnrouteDecorator):
    """For testing purposes."""


class TestHttpEnrouteDecorator(unittest.TestCase):
    def test_is_subclass(self):
        self.assertTrue(issubclass(HttpEnrouteDecorator, EnrouteDecorator))

    def test_constructor(self):
        decorator = _HttpEnrouteDecorator("/products", "GET")
        self.assertEqual("/products", decorator.url)
        self.assertEqual("GET", decorator.method)


if __name__ == "__main__":
    unittest.main()

import unittest
import warnings

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
        self.assertEqual("/products", decorator.path)
        self.assertEqual("GET", decorator.method)

    def test_constructor_with_url(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            decorator = _HttpEnrouteDecorator(url="/products", method="GET")
        self.assertEqual("/products", decorator.path)
        self.assertEqual("GET", decorator.method)

    def test_no_path_raises(self):
        with self.assertRaises(ValueError):
            _HttpEnrouteDecorator(method="GET")

    def test_no_method_raises(self):
        with self.assertRaises(ValueError):
            _HttpEnrouteDecorator(path="/products")

    def test_url(self):
        decorator = _HttpEnrouteDecorator("/products", "GET")
        self.assertEqual("/products", decorator.url)


if __name__ == "__main__":
    unittest.main()

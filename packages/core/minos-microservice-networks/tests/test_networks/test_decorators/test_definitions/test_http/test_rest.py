import unittest

from minos.networks import (
    EnrouteDecoratorKind,
    HttpEnrouteDecorator,
    RestCommandEnrouteDecorator,
    RestEnrouteDecorator,
    RestQueryEnrouteDecorator,
)


class TestRestEnrouteDecorator(unittest.TestCase):
    def test_is_subclass(self):
        self.assertTrue(issubclass(RestEnrouteDecorator, HttpEnrouteDecorator))


class TestRestCommandEnrouteDecorator(unittest.TestCase):
    def test_is_subclass(self):
        self.assertTrue(issubclass(RestCommandEnrouteDecorator, RestEnrouteDecorator))

    def test_kind(self):
        self.assertEqual(EnrouteDecoratorKind.Command, RestCommandEnrouteDecorator.KIND)


class TestRestQueryEnrouteDecorator(unittest.TestCase):
    def test_is_subclass(self):
        self.assertTrue(issubclass(RestQueryEnrouteDecorator, RestEnrouteDecorator))

    def test_kind(self):
        self.assertEqual(EnrouteDecoratorKind.Query, RestQueryEnrouteDecorator.KIND)


if __name__ == "__main__":
    unittest.main()

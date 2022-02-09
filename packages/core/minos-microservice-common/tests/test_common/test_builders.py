import unittest
from abc import (
    ABC,
)
from typing import (
    Any,
)

from minos.common import (
    Builder,
    MinosConfig,
    SetupMixin,
)
from tests.utils import (
    CONFIG_FILE_PATH,
)


class _Builder(Builder[dict[str, Any]]):
    def build(self) -> dict[str, Any]:
        """For testing purposes."""
        return self.kwargs


class TestBuilder(unittest.TestCase):
    def test_abstract(self):
        self.assertTrue(issubclass(Builder, (ABC, SetupMixin)))
        # noinspection PyUnresolvedReferences
        self.assertEqual({"build"}, Builder.__abstractmethods__)

    def test_new(self):
        builder = _Builder.new()
        self.assertIsInstance(builder, _Builder)
        self.assertEqual(dict(), builder.kwargs)

    def test_copy(self):
        base = _Builder.new().with_kwargs({"one": "two"})
        builder = base.copy()
        self.assertNotEqual(id(base), id(builder))
        self.assertIsInstance(builder, _Builder)
        self.assertEqual({"one": "two"}, builder.kwargs)

    def test_with_kwargs(self):
        builder = _Builder().with_kwargs({"foo": "bar"})
        self.assertIsInstance(builder, _Builder)
        self.assertEqual({"foo": "bar"}, builder.kwargs)

    def test_with_config(self):
        config = MinosConfig(CONFIG_FILE_PATH)
        builder = _Builder().with_config(config)
        self.assertIsInstance(builder, _Builder)
        self.assertEqual(dict(), builder.kwargs)

    def test_build(self):
        builder = _Builder().with_kwargs({"one": "two"})
        self.assertIsInstance(builder, _Builder)
        self.assertEqual({"one": "two"}, builder.build())


if __name__ == "__main__":
    unittest.main()

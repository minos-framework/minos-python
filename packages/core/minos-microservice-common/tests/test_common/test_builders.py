import unittest

from minos.common import (
    Builder,
    Config,
)
from tests.utils import (
    CONFIG_FILE_PATH,
)


class TestBuilder(unittest.TestCase):
    def test_new(self):
        builder = Builder.new().with_cls(dict)
        self.assertIsInstance(builder, Builder)
        self.assertEqual(dict(), builder.kwargs)

    def test_copy(self):
        base = Builder.new().with_cls(dict).with_kwargs({"one": "two"})
        builder = base.copy()
        self.assertNotEqual(id(base), id(builder))
        self.assertIsInstance(builder, Builder)
        self.assertEqual({"one": "two"}, builder.kwargs)
        self.assertEqual(dict, builder.instance_cls)

    def test_with_kwargs(self):
        builder = Builder().with_cls(dict).with_kwargs({"foo": "bar"})
        self.assertIsInstance(builder, Builder)
        self.assertEqual({"foo": "bar"}, builder.kwargs)

    def test_with_config(self):
        config = Config(CONFIG_FILE_PATH)
        builder = Builder().with_cls(dict).with_config(config)
        self.assertIsInstance(builder, Builder)
        self.assertEqual(dict(), builder.kwargs)

    def test_build(self):
        builder = Builder().with_cls(dict).with_kwargs({"one": "two"})
        self.assertIsInstance(builder, Builder)
        self.assertEqual({"one": "two"}, builder.build())


if __name__ == "__main__":
    unittest.main()

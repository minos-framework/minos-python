import unittest
from typing import (
    Generic,
    TypeVar,
)

from minos.common import (
    BuildableMixin,
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

    def test_str(self):
        builder = Builder().with_cls(dict).with_kwargs({"one": "two"})

        self.assertEqual("Builder(dict, {'one': 'two'})", repr(builder))

    def test_cmp(self):
        base = Builder().with_cls(dict).with_kwargs({"one": "two"})

        one = Builder().with_cls(dict).with_kwargs({"one": "two"})
        self.assertEqual(base, one)

        two = Builder().with_cls(int).with_kwargs({"one": "two"})
        self.assertNotEqual(base, two)

        three = Builder().with_cls(dict).with_kwargs({"three": "four"})
        self.assertNotEqual(base, three)

    def test_instance_cls_from_generic(self):
        class _Builder(Builder):
            """For Testing purposes."""

        class _Builder2(Builder[int]):
            """For Testing purposes."""

        class _Builder3(_Builder2):
            """For Testing purposes."""

        T = TypeVar("T")

        class _Builder4(_Builder2, Generic[T]):
            """For Testing purposes."""

        class _Builder5(_Builder4[float]):
            """For Testing purposes."""

        self.assertEqual(None, _Builder().instance_cls)
        self.assertEqual(int, _Builder2().instance_cls)
        self.assertEqual(int, _Builder3().instance_cls)
        self.assertEqual(None, _Builder4().instance_cls)
        self.assertEqual(float, _Builder5().instance_cls)


class TestBuildableMixin(unittest.TestCase):
    def test_get_builder_default(self):
        class _Foo(BuildableMixin):
            """For Testing purposes."""

        self.assertEqual(Builder().with_cls(_Foo), _Foo.get_builder())

    def test_get_builder_custom_type(self):
        class _Foo(BuildableMixin):
            """For Testing purposes."""

        class _Builder(Builder):
            """For Testing purposes."""

        _Foo.set_builder(_Builder)

        self.assertEqual(_Builder().with_cls(_Foo), _Foo.get_builder())

    def test_get_builder_custom_instance(self):
        class _Foo(BuildableMixin):
            """For Testing purposes."""

        class _Builder(Builder):
            """For Testing purposes."""

        _Foo.set_builder(_Builder().with_kwargs({"foo": "bar"}))

        self.assertEqual(_Builder().with_cls(_Foo).with_kwargs({"foo": "bar"}), _Foo.get_builder())

    def test_set_builder_raises(self):
        class _Foo(BuildableMixin):
            """For Testing purposes."""

        with self.assertRaises(ValueError):
            # noinspection PyTypeChecker
            _Foo.set_builder(int)

    def test_from_config(self):
        class _Foo(BuildableMixin):
            """For Testing purposes."""

            # noinspection PyShadowingNames
            def __init__(self, config):
                super().__init__()
                self.config = config

        class _Builder(Builder):
            """For Testing purposes."""

            def with_config(self, config: Config):
                """For Testing purposes."""
                self.kwargs["config"] = config
                return super().with_config(config)

        _Foo.set_builder(_Builder)

        config = Config(CONFIG_FILE_PATH)
        foo = _Foo.from_config(config)

        self.assertEqual(config, foo.config)


if __name__ == "__main__":
    unittest.main()

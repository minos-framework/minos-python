import unittest
from typing import (
    Generic,
    TypeVar,
)

from minos.common import (
    Injectable,
    InjectableMixin,
)

K = TypeVar("K")


@Injectable("foo")
class _Foo(int):
    """For testing purposes."""


@Injectable("bar")
class _Bar(Generic[K]):
    """For testing purposes."""


class _FooBar(_Foo, _Bar[int]):
    """For testing purposes."""


class TestInjection(unittest.TestCase):
    def test_name(self):
        decorator = Injectable("foo")
        self.assertEqual("foo", decorator.name)

    def test_name_empty(self):
        with self.assertRaises(ValueError):
            # noinspection PyTypeChecker
            Injectable(None)

    def test_injected(self):
        self.assertTrue(issubclass(_Foo, (int, InjectableMixin)))
        self.assertEqual("foo", _Foo.get_injectable_name())

        self.assertTrue(issubclass(_Bar, InjectableMixin))
        self.assertEqual("foo", _Foo.get_injectable_name())

        self.assertTrue(issubclass(_FooBar, (_Foo, _Bar, InjectableMixin)))
        self.assertEqual("foo", _FooBar.get_injectable_name())


if __name__ == "__main__":
    unittest.main()

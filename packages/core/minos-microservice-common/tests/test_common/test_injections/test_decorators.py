import unittest
from typing import (
    Generic,
    TypeVar,
    Union,
)

from minos.common import (
    DependencyInjector,
    Inject,
    Injectable,
    InjectableMixin,
    NotProvidedException,
)

K = TypeVar("K")


@Injectable("foo")
class _Foo(int):
    """For testing purposes."""


@Injectable("bar")
class _Bar(Generic[K]):
    """For testing purposes."""

    value: K

    def __init__(self, value):
        self.value = value


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


@Inject()
def _get_foo_sync(foo: _Foo) -> _Foo:
    return foo


@Inject()
async def _get_foo_async(foo: _Foo) -> _Foo:
    return foo


# noinspection PyUnusedLocal
@Inject()
def _get_foo_with_not_injectable(foo: _Foo, another: int) -> int:
    return another


@Inject()
def _get_bar(bar: Union[_Bar, int]) -> Union[_Bar[int], int]:
    return bar


@Inject()
def _get_bar_with_default(bar: Union[_Bar[int], int] = 12) -> int:
    if isinstance(bar, _Bar):
        bar = bar.value
    return bar


class TestInject(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.foo = _Foo(34)
        self.injector = DependencyInjector(None, [self.foo])
        self.injector.wire_injections()

    def tearDown(self) -> None:
        self.injector.unwire_injections()
        super().tearDown()

    def test_decorator_sync(self):
        self.assertEqual(self.foo, _get_foo_sync())

    def test_decorator_sync_arg(self):
        another = _Foo(56)
        self.assertEqual(another, _get_foo_sync(another))

    def test_decorator_sync_kwarg(self):
        another = _Foo(56)
        self.assertEqual(another, _get_foo_sync(foo=another))

    async def test_decorator_async(self):
        self.assertEqual(self.foo, await _get_foo_async())

    async def test_decorator_async_arg(self):
        another = _Foo(56)
        self.assertEqual(another, await _get_foo_async(another))

    async def test_decorator_async_kwarg(self):
        another = _Foo(56)
        self.assertEqual(another, await _get_foo_async(foo=another))

    def test_decorator_not_provided(self):
        with self.assertRaises(NotProvidedException):
            _get_bar()

    def test_decorator_ignore_not_injectable(self):
        self.assertEqual(56, _get_foo_with_not_injectable(another=56))

    def test_decorator_not_provided_with_default(self):
        self.assertEqual(12, _get_bar_with_default())

    def test_resolve_by_name(self):
        self.assertEqual(self.foo, Inject.resolve_by_name("foo"))

    def test_resolve_by_name_raises(self):
        with self.assertRaises(NotProvidedException):
            Inject.resolve_by_name("bar")


if __name__ == "__main__":
    unittest.main()

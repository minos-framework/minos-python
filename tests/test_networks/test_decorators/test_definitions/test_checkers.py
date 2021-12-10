import unittest
from datetime import (
    timedelta,
)

from minos.networks import (
    CheckDecorator,
    CheckerMeta,
    CheckerWrapper,
    Request,
)
from tests.utils import (
    FakeRequest,
    FakeService,
)


# noinspection PyUnusedLocal
def _fn(request: Request) -> bool:
    """For testing purposes."""
    return True


# noinspection PyUnusedLocal
async def _async_fn(request: Request) -> bool:
    """For testing purposes."""
    return True


class TestEnrouteCheckDecorator(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.request = FakeRequest("test")
        self.max_attempts = 30
        self.delay = 1
        self.decorator = CheckDecorator(self.max_attempts, self.delay)

    def test_constructor(self):
        self.assertEqual(self.max_attempts, self.decorator.max_attempts)
        self.assertEqual(self.delay, self.decorator.delay)
        self.assertEqual(None, self.decorator._checkers)
        self.assertEqual(None, self.decorator._handler)

    def test_constructor_extended(self):
        checkers = set()
        handler = FakeService.create_ticket
        decorator = CheckDecorator(self.max_attempts, self.delay, checkers, handler)

        self.assertEqual(self.max_attempts, decorator.max_attempts)
        self.assertEqual(self.delay, decorator.delay)
        self.assertEqual(checkers, decorator._checkers)
        self.assertEqual(handler, decorator._handler)

    def test_decorate(self):
        decorated = self.decorator(_fn)
        self.assertIsInstance(decorated, CheckerWrapper)
        self.assertEqual(CheckerMeta(_fn, self.max_attempts, self.delay), decorated.meta)

    def test_decorate_add_checkers(self):
        checkers = set()
        decorator = CheckDecorator(self.max_attempts, self.delay, _checkers=checkers)
        decorator(_fn)
        self.assertEqual({CheckerMeta(_fn, self.max_attempts, self.delay)}, checkers)

    def test_decorate_raises(self):
        decorator = CheckDecorator(self.max_attempts, self.delay, _handler=FakeService.create_ticket)
        with self.assertRaises(ValueError):
            decorator(_async_fn)

    def test_delay_timedelta(self):
        decorator = CheckDecorator(self.max_attempts, timedelta(seconds=2, milliseconds=500))
        self.assertEqual(2.5, decorator.delay)

    def test_iter(self):
        self.assertEqual((self.max_attempts, self.delay), tuple(self.decorator))

    def test_hash(self):
        self.assertEqual(hash((self.max_attempts, self.delay)), hash(self.decorator))

    def test_eq(self):
        self.assertEqual(CheckDecorator(self.max_attempts, self.delay), CheckDecorator(self.max_attempts, self.delay))
        self.assertNotEqual(CheckDecorator(2, self.delay), CheckDecorator(1, self.delay))
        self.assertNotEqual(CheckDecorator(self.max_attempts, 0.5), CheckDecorator(self.max_attempts, 0.6))

    def test_repr(self):
        self.assertEqual(f"CheckDecorator({self.max_attempts!r}, {self.delay!r})", repr(self.decorator))


if __name__ == "__main__":
    unittest.main()

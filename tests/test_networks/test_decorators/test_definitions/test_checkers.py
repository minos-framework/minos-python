import unittest
from datetime import (
    timedelta,
)

from minos.networks import (
    CheckDecorator,
    CheckerMeta,
    CheckerWrapper,
    InMemoryRequest,
    Request,
    enroute,
)
from tests.utils import (
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
        self.request = InMemoryRequest("test")
        self.max_attempts = 30
        self.delay = 1
        self.handler_meta = FakeService.create_ticket.meta
        self.decorator = CheckDecorator(self.handler_meta, self.max_attempts, self.delay)

    def test_constructor(self):
        decorator = CheckDecorator(self.handler_meta, self.max_attempts, self.delay)

        self.assertEqual(self.max_attempts, decorator.max_attempts)
        self.assertEqual(self.delay, decorator.delay)
        self.assertEqual(self.handler_meta, decorator.handler_meta)

    def test_decorate(self):
        decorated = self.decorator(_fn)
        self.assertIsInstance(decorated, CheckerWrapper)
        self.assertEqual(CheckerMeta(_fn, self.max_attempts, self.delay), decorated.meta)

    def test_decorate_add_checkers(self):
        handler_meta = enroute.broker.command("CreateTicket")(_async_fn).meta
        decorator = CheckDecorator(handler_meta, self.max_attempts, self.delay)
        decorator(_fn)
        self.assertEqual({CheckerMeta(_fn, self.max_attempts, self.delay)}, handler_meta.checkers)

    def test_decorate_raises(self):
        decorator = CheckDecorator(self.handler_meta, self.max_attempts, self.delay)
        with self.assertRaises(ValueError):
            decorator(_async_fn)

    def test_delay_timedelta(self):
        decorator = CheckDecorator(self.handler_meta, self.max_attempts, timedelta(seconds=2, milliseconds=500))
        self.assertEqual(2.5, decorator.delay)

    def test_iter(self):
        self.assertEqual((self.handler_meta, self.max_attempts, self.delay), tuple(self.decorator))

    def test_hash(self):
        self.assertEqual(hash((self.handler_meta, self.max_attempts, self.delay)), hash(self.decorator))

    def test_eq(self):
        self.assertEqual(
            CheckDecorator(self.handler_meta, self.max_attempts, self.delay),
            CheckDecorator(self.handler_meta, self.max_attempts, self.delay),
        )
        self.assertNotEqual(
            CheckDecorator(enroute.broker.event("TicketCreated")(_fn).meta, self.max_attempts, self.delay),
            CheckDecorator(enroute.broker.event("TicketUpdated")(_async_fn).meta, self.max_attempts, self.delay),
        )
        self.assertNotEqual(
            CheckDecorator(self.handler_meta, 2, self.delay), CheckDecorator(self.handler_meta, 1, self.delay)
        )
        self.assertNotEqual(
            CheckDecorator(self.handler_meta, self.max_attempts, 0.5),
            CheckDecorator(self.handler_meta, self.max_attempts, 0.6),
        )

    def test_repr(self):
        self.assertEqual(
            f"CheckDecorator({self.handler_meta!r}, {self.max_attempts!r}, {self.delay!r})", repr(self.decorator)
        )


if __name__ == "__main__":
    unittest.main()

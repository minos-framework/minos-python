import unittest
from unittest.mock import (
    AsyncMock,
    MagicMock,
)

from minos.networks import (
    CheckerMeta,
    CheckerWrapper,
    Request,
)
from tests.utils import (
    FakeRequest,
)


def _fn(request: Request) -> bool:
    """For testing purposes."""
    # noinspection PyProtectedMember,PyUnresolvedReferences
    return request._content


# noinspection PyUnusedLocal
async def _async_fn(request: Request) -> bool:
    """For testing purposes."""
    return await request.content()


class TestCheckerMeta(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.max_attempts = 3
        self.delay = 0.050
        self.meta = CheckerMeta(_fn, self.max_attempts, self.delay)

    def test_constructor(self):
        self.assertEqual(_fn, self.meta.func)
        self.assertEqual(self.max_attempts, self.meta.max_attempts)
        self.assertEqual(self.delay, self.meta.delay)

    async def test_wrapper_async(self):
        meta = CheckerMeta(_async_fn, self.max_attempts, self.delay)
        wrapper = meta.wrapper
        self.assertIsInstance(wrapper, CheckerWrapper)
        self.assertEqual(meta, wrapper.meta)

    def test_wrapper_sync(self):
        meta = CheckerMeta(_fn, self.max_attempts, self.delay)
        wrapper = meta.wrapper
        self.assertIsInstance(wrapper, CheckerWrapper)
        self.assertEqual(meta, wrapper.meta)

    async def test_wrapper_async_call_true(self):
        mock = AsyncMock(side_effect=[True])
        meta = CheckerMeta(mock, self.max_attempts, self.delay)
        self.assertEqual(True, await meta.wrapper(FakeRequest(True)))
        self.assertEqual(1, mock.call_count)

    async def test_wrapper_async_call_true_corner(self):
        mock = AsyncMock(side_effect=[False, False, True])
        meta = CheckerMeta(mock, self.max_attempts, self.delay)
        self.assertEqual(True, await meta.wrapper(FakeRequest(True)))
        self.assertEqual(3, mock.call_count)

    async def test_wrapper_async_call_false(self):
        mock = AsyncMock(side_effect=[False, False, False])
        meta = CheckerMeta(mock, self.max_attempts, self.delay)
        self.assertEqual(False, await meta.wrapper(FakeRequest(False)))
        self.assertEqual(self.max_attempts, mock.call_count)

    def test_wrapper_sync_call_true(self):
        mock = MagicMock(side_effect=[True])
        meta = CheckerMeta(mock, self.max_attempts, self.delay)
        self.assertEqual(True, meta.wrapper(FakeRequest(True)))
        self.assertEqual(1, mock.call_count)

    def test_wrapper_sync_call_true_corner(self):
        mock = MagicMock(side_effect=[False, False, True])
        meta = CheckerMeta(mock, self.max_attempts, self.delay)
        self.assertEqual(True, meta.wrapper(FakeRequest(True)))
        self.assertEqual(3, mock.call_count)

    def test_wrapper_sync_call_false(self):
        mock = MagicMock(side_effect=[False, False, False])
        meta = CheckerMeta(mock, self.max_attempts, self.delay)
        self.assertEqual(False, meta.wrapper(FakeRequest(False)))
        self.assertEqual(self.max_attempts, mock.call_count)

    def test_repr(self):
        self.assertEqual(f"CheckerMeta({_fn!r}, {self.max_attempts!r}, {self.delay!r})", repr(self.meta))

    def test_eq(self):
        self.assertEqual(
            CheckerMeta(_fn, self.max_attempts, self.delay), CheckerMeta(_fn, self.max_attempts, self.delay)
        )
        self.assertNotEqual(
            CheckerMeta(_fn, self.max_attempts, self.delay), CheckerMeta(_async_fn, self.max_attempts, self.delay)
        )
        self.assertNotEqual(CheckerMeta(_fn, 1, self.delay), CheckerMeta(_fn, 2, self.delay))
        self.assertNotEqual(CheckerMeta(_fn, self.max_attempts, 1), CheckerMeta(_fn, self.max_attempts, 2))

    def test_iter(self):
        self.assertEqual((_fn, self.max_attempts, self.delay), tuple(self.meta))

    def test_hash(self):
        self.assertEqual(hash((_fn, self.max_attempts, self.delay)), hash(self.meta))


if __name__ == "__main__":
    unittest.main()

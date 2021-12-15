import unittest
from unittest.mock import (
    AsyncMock,
    MagicMock,
)

from minos.networks import (
    CheckerMeta,
    CheckerWrapper,
    NotSatisfiedCheckerException,
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
        mock = AsyncMock(return_value=True)
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
        mock = MagicMock(return_value=True)
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

    async def test_async_wrapper_sync(self):
        mock = MagicMock(return_value=True)
        meta = CheckerMeta(mock, self.max_attempts, self.delay)
        self.assertEqual(True, await meta.async_wrapper(FakeRequest(True)))

    def test_sync_wrapper_async_raises(self):
        mock = AsyncMock(return_value=True)
        meta = CheckerMeta(mock, self.max_attempts, self.delay)
        with self.assertRaises(ValueError):
            meta.sync_wrapper(FakeRequest(True))

    async def test_run_async(self):
        m1 = CheckerMeta(AsyncMock(return_value=True), self.max_attempts, self.delay)
        m2 = CheckerMeta(MagicMock(return_value=True), self.max_attempts, self.delay)
        await CheckerMeta.run_async({m1, m2}, FakeRequest(True))
        self.assertTrue(True)

    async def test_run_async_raises(self):
        m1 = CheckerMeta(AsyncMock(return_value=True), self.max_attempts, self.delay)
        m2 = CheckerMeta(MagicMock(return_value=False), self.max_attempts, self.delay)
        with self.assertRaises(NotSatisfiedCheckerException):
            await CheckerMeta.run_async({m1, m2}, FakeRequest(True))

    def test_run_sync(self):
        m1 = CheckerMeta(MagicMock(return_value=True), self.max_attempts, self.delay)
        m2 = CheckerMeta(MagicMock(return_value=True), self.max_attempts, self.delay)
        CheckerMeta.run_sync({m1, m2}, FakeRequest(True))
        self.assertTrue(True)

    def test_run_sync_raises(self):
        m1 = CheckerMeta(MagicMock(return_value=True), self.max_attempts, self.delay)
        m2 = CheckerMeta(MagicMock(return_value=False), self.max_attempts, self.delay)
        with self.assertRaises(NotSatisfiedCheckerException):
            CheckerMeta.run_sync({m1, m2}, FakeRequest(True))

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

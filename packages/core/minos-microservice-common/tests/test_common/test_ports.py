import unittest
from unittest.mock import (
    AsyncMock,
    call,
)

from aiomisc import (
    Service,
)

from minos.common import (
    Port,
)


class _Port(Port):
    """For testing purposes."""

    def _start(self):
        """For testing purposes."""

    async def _stop(self, err: Exception = None) -> None:
        """For testing purposes."""


class TestPort(unittest.IsolatedAsyncioTestCase):
    def test_is_subclass(self):
        self.assertTrue(issubclass(Port, Service))

    def test_abstract(self):
        # noinspection PyUnresolvedReferences
        self.assertEqual({"_start", "_stop"}, Port.__abstractmethods__)

    async def test_start(self):
        port = _Port()

        mock = AsyncMock()
        port._start = mock

        await port.start()

        self.assertEqual([call()], mock.call_args_list)

    async def test_start_raises(self):
        port = _Port()

        mock = AsyncMock(side_effect=ValueError)
        port._start = mock

        with self.assertRaises(ValueError):
            await port.start()

        self.assertEqual([call()], mock.call_args_list)

    async def test_stop(self):
        port = _Port()

        mock = AsyncMock()
        port._stop = mock

        await port.stop()

        self.assertEqual([call(None)], mock.call_args_list)

    async def test_stop_raises(self):
        port = _Port()

        mock = AsyncMock(side_effect=ValueError)
        port._stop = mock

        with self.assertRaises(ValueError):
            await port.stop()

        self.assertEqual([call(None)], mock.call_args_list)


if __name__ == "__main__":
    unittest.main()

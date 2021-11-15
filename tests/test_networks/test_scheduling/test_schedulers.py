import asyncio
import unittest
import warnings
from unittest.mock import (
    AsyncMock,
    call,
    patch,
)

from crontab import (
    CronTab,
)

from minos.common import (
    MinosConfig,
    current_datetime,
)
from minos.networks import (
    PeriodicTask,
    PeriodicTaskScheduler,
    ScheduledRequest,
    ScheduledResponseException,
)
from tests.utils import (
    BASE_PATH,
)


class TestPeriodicTaskScheduler(unittest.IsolatedAsyncioTestCase):
    def test_from_config(self):
        config = MinosConfig(BASE_PATH / "test_config.yml")
        scheduler = PeriodicTaskScheduler.from_config(config)
        self.assertEqual(1, len(scheduler.tasks))
        self.assertTrue(all(map(lambda t: isinstance(t, PeriodicTask), scheduler.tasks)))

    def test_tasks(self):
        # noinspection PyTypeChecker
        tasks = {PeriodicTask("@daily", None), PeriodicTask("@hourly", None)}

        scheduler = PeriodicTaskScheduler(tasks)

        self.assertEqual(tasks, scheduler.tasks)

    async def test_start(self):
        tasks_1_mock = AsyncMock()
        tasks_2_mock = AsyncMock()

        tasks = {tasks_1_mock, tasks_2_mock}

        scheduler = PeriodicTaskScheduler(tasks)

        await scheduler.start()

        self.assertEqual(1, tasks_1_mock.start.call_count)
        self.assertEqual(call(), tasks_1_mock.start.call_args)

        self.assertEqual(1, tasks_2_mock.start.call_count)
        self.assertEqual(call(), tasks_2_mock.start.call_args)

    async def test_stop(self):
        tasks_1_mock = AsyncMock()
        tasks_2_mock = AsyncMock()

        tasks = {tasks_1_mock, tasks_2_mock}

        scheduler = PeriodicTaskScheduler(tasks)

        await scheduler.stop(timeout=30)

        self.assertEqual(1, tasks_1_mock.stop.call_count)
        self.assertEqual(call(timeout=30), tasks_1_mock.stop.call_args)

        self.assertEqual(1, tasks_2_mock.stop.call_count)
        self.assertEqual(call(timeout=30), tasks_2_mock.stop.call_args)


class TestPeriodicTask(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.fn_mock = AsyncMock()
        self.periodic = PeriodicTask("@daily", self.fn_mock)

    def test_crontab(self) -> None:
        self.assertEqual(CronTab("@daily").matchers, self.periodic.crontab.matchers)

    def test_fn(self) -> None:
        self.assertEqual(self.fn_mock, self.periodic.fn)

    async def test_start(self) -> None:
        self.assertFalse(self.periodic.started)

        with patch("asyncio.create_task", return_value="test") as mock_create:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", RuntimeWarning)
                await self.periodic.start()
            self.assertEqual(1, mock_create.call_count)
            self.assertEqual("run_forever", mock_create.call_args.args[0].__name__)
            self.assertTrue(self.periodic.started)
            self.assertEqual("test", self.periodic.task)

    async def test_stop(self) -> None:
        mock = AsyncMock()
        self.periodic._task = mock
        with patch("asyncio.wait_for") as mock_wait:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", RuntimeWarning)
                await self.periodic.stop()
            self.assertEqual(1, mock_wait.call_count)
            self.assertEqual(call(mock, None), mock_wait.call_args)

    async def test_run_forever(self) -> None:
        with patch("asyncio.sleep") as mock_sleep:
            run_once_mock = AsyncMock(side_effect=ValueError)
            self.periodic.run_once = run_once_mock

            with self.assertRaises(ValueError):
                await self.periodic.run_forever()

            self.assertEqual(2, mock_sleep.call_count)
            self.assertEqual(1, run_once_mock.call_count)

    async def test_run_once(self) -> None:
        now = current_datetime()
        await self.periodic.run_once(now)
        self.assertEqual(1, self.fn_mock.call_count)

        observed = self.fn_mock.call_args.args[0]
        self.assertIsInstance(observed, ScheduledRequest)
        self.assertEqual(now, (await observed.content()).scheduled_at)

    async def test_run_once_handle_exceptions(self) -> None:
        self.fn_mock.side_effect = asyncio.CancelledError
        await self.periodic.run_once()

        self.fn_mock.side_effect = Exception
        await self.periodic.run_once()

        self.fn_mock.side_effect = ScheduledResponseException("")
        await self.periodic.run_once()

        self.assertTrue(True)

    async def test_run_once_running(self) -> None:
        def _fn(*args, **kwargs):
            self.assertTrue(self.periodic.running)

        self.fn_mock.side_effect = _fn

        self.assertFalse(self.periodic.running)
        await self.periodic.run_once()
        self.assertFalse(self.periodic.running)

        self.assertEqual(1, self.fn_mock.call_count)


if __name__ == "__main__":
    unittest.main()

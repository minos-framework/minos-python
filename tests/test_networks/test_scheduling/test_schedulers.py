import unittest
from unittest.mock import (
    AsyncMock,
    call,
)

from minos.common import (
    MinosConfig,
)
from minos.networks import (
    PeriodicTask,
    TaskScheduler,
)
from tests.utils import (
    BASE_PATH,
)


class TestTaskScheduler(unittest.IsolatedAsyncioTestCase):
    def test_from_config(self):
        config = MinosConfig(BASE_PATH / "test_config.yml")
        scheduler = TaskScheduler.from_config(config)
        self.assertEqual(1, len(scheduler.tasks))
        self.assertTrue(all(map(lambda t: isinstance(t, PeriodicTask), scheduler.tasks)))

    def test_tasks(self):
        tasks = {PeriodicTask("@daily", None), PeriodicTask("@hourly", None)}

        scheduler = TaskScheduler(tasks)

        self.assertEqual(tasks, scheduler.tasks)

    async def test_start(self):
        tasks_1_mock = AsyncMock()
        tasks_2_mock = AsyncMock()

        tasks = {tasks_1_mock, tasks_2_mock}

        scheduler = TaskScheduler(tasks)

        await scheduler.start()

        self.assertEqual(1, tasks_1_mock.start.call_count)
        self.assertEqual(call(), tasks_1_mock.start.call_args)

        self.assertEqual(1, tasks_2_mock.start.call_count)
        self.assertEqual(call(), tasks_2_mock.start.call_args)

    async def test_stop(self):
        tasks_1_mock = AsyncMock()
        tasks_2_mock = AsyncMock()

        tasks = {tasks_1_mock, tasks_2_mock}

        scheduler = TaskScheduler(tasks)

        await scheduler.stop(timeout=30)

        self.assertEqual(1, tasks_1_mock.stop.call_count)
        self.assertEqual(call(timeout=30), tasks_1_mock.stop.call_args)

        self.assertEqual(1, tasks_2_mock.stop.call_count)
        self.assertEqual(call(timeout=30), tasks_2_mock.stop.call_args)


class TestPeriodicTask(unittest.IsolatedAsyncioTestCase):
    pass


if __name__ == "__main__":
    unittest.main()

from __future__ import (
    annotations,
)

import asyncio
import logging
from contextlib import (
    suppress,
)
from datetime import (
    datetime,
)
from typing import (
    Awaitable,
    Callable,
    NoReturn,
    Optional,
    Union,
)

from crontab import (
    CronTab,
)

from minos.common import (
    MinosConfig,
    MinosSetup,
    current_datetime,
)

from ..decorators import (
    EnrouteBuilder,
)
from .messages import (
    SchedulingRequest,
)

logger = logging.getLogger(__name__)


class TaskScheduler(MinosSetup):
    """TODO"""

    def __init__(self, tasks: set[PeriodicTask], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._tasks = tasks

    @classmethod
    def _from_config(cls, config: MinosConfig, **kwargs) -> TaskScheduler:
        tasks = cls._tasks_from_config(config, **kwargs)
        return cls(tasks, **kwargs)

    @staticmethod
    def _tasks_from_config(config: MinosConfig, **kwargs) -> set[PeriodicTask]:
        builder = EnrouteBuilder(config.commands.service, config.queries.service)
        decorators = builder.get_periodic_event(config=config, **kwargs)
        tasks = {PeriodicTask(decorator.crontab, fn) for decorator, fn in decorators.items()}
        return tasks

    @property
    def tasks(self) -> set[PeriodicTask]:
        """TODO

        :return: TODO
        """
        return self._tasks

    async def start(self) -> None:
        """TODO

        :return: TODO
        """

        await asyncio.gather(*(task.start() for task in self._tasks))

    async def stop(self, timeout: Optional[float] = None) -> None:
        """TODO

        :param timeout: TODO
        :return: TODO
        """
        await asyncio.gather(*(task.stop(timeout=timeout) for task in self._tasks))


class PeriodicTask:
    """TODO"""

    _task: Optional[asyncio.Task]

    def __init__(self, crontab: Union[str, CronTab], fn: Callable[[SchedulingRequest], Awaitable[None]]):
        if isinstance(crontab, str):
            crontab = CronTab(crontab)

        self._crontab = crontab
        self._fn = fn
        self._task = None
        self._running = False

    @property
    def crontab(self) -> CronTab:
        """TODO

        :return: TODO
        """
        return self._crontab

    @property
    def fn(self) -> Callable[[SchedulingRequest], Awaitable[None]]:
        """TODO

        :return: TODO
        """
        return self._fn

    @property
    def started(self) -> bool:
        """TODO

        :return: TODO
        """

        return self._task is not None

    @property
    def task(self) -> asyncio.Task:
        """TODO

        :return: TODO
        """
        return self._task

    async def start(self) -> None:
        """TODO

        :return: TODO
        """
        self._task = asyncio.create_task(self.run_forever())

    async def stop(self, timeout: Optional[float] = None) -> None:
        """TODO

        :param timeout: TODO
        :return: TODO
        """
        if self._task is not None:
            self._task.cancel()
            with suppress(asyncio.TimeoutError, asyncio.CancelledError):
                await asyncio.wait_for(self._task, timeout)
            self._task = None

    async def run_forever(self) -> NoReturn:
        """TODO

        :return: TODO
        """
        now = current_datetime()
        await asyncio.sleep(self._crontab.next(now))

        while True:
            now = current_datetime()
            await asyncio.gather(asyncio.sleep(self._crontab.next(now)), self.run_once(now))

    @property
    def running(self) -> bool:
        """TODO

        :return: TODO
        """
        return self._running

    async def run_once(self, now: Optional[datetime] = None) -> None:
        """TODO

        :param now: TODO
        :return: TODO
        """
        if now is None:
            now = current_datetime()

        request = SchedulingRequest(now)
        try:
            self._running = True
            with suppress(asyncio.CancelledError):
                await self._fn(request)
        except Exception as exc:
            logger.warning(f"Raised exception while executing periodic task: {exc}")
        finally:
            self._running = False

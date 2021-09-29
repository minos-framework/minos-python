from __future__ import (
    annotations,
)

import logging
from asyncio import (
    CancelledError,
    Task,
    TimeoutError,
    create_task,
    gather,
    sleep,
    wait_for,
)
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


class PeriodicTask:
    """TODO"""

    _task: Optional[Task]

    def __init__(self, crontab: CronTab, fn: Callable[[SchedulingRequest], Awaitable[None]]):
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

    async def start(self) -> None:
        """TODO

        :return: TODO
        """
        self._task = create_task(self.run_forever())

    @property
    def started(self) -> bool:
        """TODO

        :return: TODO
        """

        return self._task is not None

    @property
    def running(self) -> bool:
        """TODO

        :return: TODO
        """
        return self._running

    async def stop(self, timeout: Optional[float] = None) -> None:
        """TODO

        :param timeout: TODO
        :return: TODO
        """
        if self._task is not None:
            self._task.cancel()
            with suppress(TimeoutError, CancelledError):
                await wait_for(self._task, timeout)
            self._task = None

    async def run_forever(self) -> NoReturn:
        """TODO

        :return: TODO
        """
        while True:
            now = current_datetime()
            await gather(self.run_one(now), sleep(self._crontab.next(now)))

    async def run_one(self, now: datetime) -> None:
        """TODO

        :param now: TODO
        :return: TODO
        """

        request = SchedulingRequest(now)
        try:
            self._running = True
            with suppress(CancelledError):
                await self._fn(request)
        except Exception as exc:
            logger.warning(f"Raised exception while executing periodic task: {exc}")
        finally:
            self._running = False


class TaskScheduler(MinosSetup):
    """TODO"""

    def __init__(self, tasks: list[PeriodicTask], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._tasks = tasks

    @classmethod
    def _from_config(cls, config: MinosConfig, **kwargs) -> TaskScheduler:
        builder = EnrouteBuilder(config.commands.service, config.queries.service)
        decorators = builder.get_periodic_event(config=config, **kwargs)
        tasks = [PeriodicTask(decorator.crontab, fn) for decorator, fn in decorators.items()]
        return cls(tasks, **kwargs)

    async def start(self) -> None:
        """TODO

        :return: TODO
        """

        await gather(*(task.start() for task in self._tasks))

    async def stop(self, timeout: Optional[float] = None) -> None:
        """TODO

        :param timeout: TODO
        :return: TODO
        """
        await gather(*(task.stop(timeout) for task in self._tasks))

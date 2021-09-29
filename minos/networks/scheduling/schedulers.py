from __future__ import (
    annotations,
)

import logging
from asyncio import (
    Task,
    create_task,
    gather,
    sleep,
    wait_for,
)
from datetime import (
    timedelta,
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
        self.crontab = crontab
        self.fn = fn
        self._task = None

    async def start(self) -> None:
        """TODO

        :return: TODO
        """
        self._task = create_task(self.callback())

    @property
    def started(self) -> bool:
        """TODO

        :return: TODO
        """

        return self._task is not None

    async def stop(self, timeout: Optional[float] = None) -> None:
        """TODO

        :param timeout: TODO
        :return: TODO
        """
        if self._task is not None:
            self._task.cancel()
            await wait_for(self._task, timeout)
            self._task = None

    async def callback(self) -> NoReturn:
        """TODO

        :return: TODO
        """

        now = current_datetime()
        while True:
            request = SchedulingRequest(now)
            try:
                await self.fn(request)
            except Exception as exc:  # TODO
                logger.warning(f"Raised exception while executing task: {exc}")
            now = current_datetime()
            delay = self.crontab.next(now)
            now += timedelta(seconds=delay)
            await sleep(delay)


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

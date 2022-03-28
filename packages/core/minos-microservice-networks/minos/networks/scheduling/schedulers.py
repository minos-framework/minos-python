from __future__ import (
    annotations,
)

import asyncio
import logging
import traceback
from contextlib import (
    suppress,
)
from datetime import (
    datetime,
)
from inspect import (
    isawaitable,
)
from typing import (
    Awaitable,
    Callable,
    NoReturn,
    Optional,
    Union,
)

from crontab import CronTab as CronTabImpl

from minos.common import (
    Config,
    SetupMixin,
    current_datetime,
)

from ..decorators import (
    EnrouteFactory,
)
from ..requests import (
    ResponseException,
)
from .crontab import (
    CronTab,
)
from .requests import (
    ScheduledRequest,
)

logger = logging.getLogger(__name__)


class PeriodicTaskScheduler(SetupMixin):
    """Periodic Task Scheduler class."""

    def __init__(self, tasks: set[PeriodicTask], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._tasks = tasks

    @classmethod
    def _from_config(cls, config: Config, **kwargs) -> PeriodicTaskScheduler:
        tasks = cls._tasks_from_config(config, **kwargs)
        return cls(tasks, **kwargs)

    @staticmethod
    def _tasks_from_config(config: Config, **kwargs) -> set[PeriodicTask]:
        builder = EnrouteFactory(*config.get_services(), middleware=config.get_middleware())
        decorators = builder.get_periodic_event(config=config, **kwargs)
        tasks = {PeriodicTask(decorator.crontab, fn) for decorator, fn in decorators.items()}
        return tasks

    @property
    def tasks(self) -> set[PeriodicTask]:
        """Get the set of periodic tasks.

        :return: A ``set`` of ``PeriodicTask`` instances.
        """
        return self._tasks

    async def start(self) -> None:
        """Start the execution of periodic tasks.

        :return: This method does not return anything.
        """

        await asyncio.gather(*(task.start() for task in self._tasks))

    async def stop(self, timeout: Optional[float] = None) -> None:
        """Stop the execution of periodic tasks.

        :param timeout: An optional timeout expressed in seconds.
        :return: This method does not return anything.
        """
        await asyncio.gather(*(task.stop(timeout=timeout) for task in self._tasks))


class PeriodicTask:
    """Periodic Task class."""

    _task: Optional[asyncio.Task]

    def __init__(self, crontab: Union[str, CronTab, CronTabImpl], fn: Callable[[ScheduledRequest], Awaitable[None]]):
        if not isinstance(crontab, CronTab):
            crontab = CronTab(crontab)

        self._crontab = crontab
        self._fn = fn
        self._task = None
        self._running = False

    @property
    def crontab(self) -> CronTab:
        """Get the crontab of the periodic task.

        :return: A ``CronTab`` instance.
        """
        return self._crontab

    @property
    def fn(self) -> Callable[[ScheduledRequest], Awaitable[None]]:
        """Get the function to be called periodically.

        :return: A function returning an awaitable.
        """
        return self._fn

    @property
    def started(self) -> bool:
        """Check if the periodic task has been started.

        :return: ``True`` if started or ``False`` otherwise.
        """

        return self._task is not None

    @property
    def task(self) -> asyncio.Task:
        """Get the asyncio task.

        :return: An ``asyncio.Task`` instance.
        """
        return self._task

    async def start(self) -> None:
        """Start the periodic task.

        :return: This method does not return anything.
        """
        logger.info("Starting periodic task...")
        self._task = asyncio.create_task(self.run_forever())

    async def stop(self, timeout: Optional[float] = None) -> None:
        """Stop the periodic task.

        :param timeout: An optional timeout expressed in seconds.
        :return: This method does not return anything.
        """
        if self._task is not None:
            logger.info("Stopping periodic task...")
            self._task.cancel()
            with suppress(asyncio.TimeoutError, asyncio.CancelledError):
                await asyncio.wait_for(self._task, timeout)
            self._task = None

    async def run_forever(self) -> NoReturn:
        """Run the periodic function forever. This method is equivalent to start, but it keeps waiting until infinite.

        :return: This method never returns.
        """

        async for now in self._crontab:
            await self.run_once(now)

    @property
    def running(self) -> bool:
        """Check if the periodic function is running.

        :return: ``True`` if it's running or ``False`` otherwise.
        """
        return self._running

    async def run_once(self, now: Optional[datetime] = None) -> None:
        """Run the periodic function one time.

        :param now: An optional datetime expressing the current datetime.
        :return: This method does not return anything.
        """
        if now is None:
            now = current_datetime()

        request = ScheduledRequest(now)
        logger.debug("Running periodic task...")
        # noinspection PyBroadException
        try:
            self._running = True
            with suppress(asyncio.CancelledError):
                response = self._fn(request)
                if isawaitable(response):
                    await response
        except ResponseException:
            tb = traceback.format_exc()
            logger.error(f"Raised an application exception:\n {tb}")
        except Exception:
            tb = traceback.format_exc()
            logger.exception(f"Raised a system exception:\n {tb}")
        finally:
            self._running = False

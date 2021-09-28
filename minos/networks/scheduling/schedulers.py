from __future__ import (
    annotations,
)

import logging
from asyncio import (
    AbstractEventLoop,
    gather,
    get_event_loop,
    sleep,
)
from datetime import (
    timedelta,
)
from itertools import (
    chain,
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


class TaskScheduler(MinosSetup):
    """TODO"""

    def __init__(
        self, tasks: list[tuple[CronTab, Callable]], loop: Optional[AbstractEventLoop] = None, *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        if loop is None:
            loop = get_event_loop()

        self._tasks = tasks
        self._loop = loop

    @classmethod
    def _from_config(cls, config: MinosConfig, **kwargs) -> TaskScheduler:
        command_decorators = EnrouteBuilder(config.commands.service, config).get_periodic_event()
        service_decorators = EnrouteBuilder(config.queries.service, config).get_periodic_event()

        tasks = [
            (decorator.crontab, fn) for decorator, fn in chain(command_decorators.items(), service_decorators.items())
        ]
        return cls(tasks)

    async def start(self) -> None:
        """TODO

        :return: TODO
        """

        await gather(*(self.callback(*task) for task in self._tasks))

    @staticmethod
    async def callback(crontab: CronTab, fn: Callable[[SchedulingRequest], Awaitable]) -> NoReturn:
        """TODO

        :param crontab: TODO
        :param fn: TODO
        :return: TODO
        """

        now = current_datetime()
        while True:
            request = SchedulingRequest(now)
            try:
                await fn(request)
            except Exception as exc:  # TODO
                logger.warning(f"Raised exception while executing task: {exc}")
            now = current_datetime()
            delay = crontab.next(now)
            now += timedelta(seconds=delay)
            await sleep(delay)

    async def stop(self) -> None:
        """TODO

        :return: TODO
        """
        pass

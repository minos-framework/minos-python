import logging
import warnings

from cached_property import (
    cached_property,
)

from minos.common import (
    Port,
)

from .schedulers import (
    PeriodicTaskScheduler,
)

logger = logging.getLogger(__name__)


class PeriodicPort(Port):
    """Periodic Port class."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._init_kwargs = kwargs

    async def _start(self) -> None:
        await self.scheduler.setup()

        try:
            self.start_event.set()
        except RuntimeError:
            logger.warning("Runtime is not properly setup.")

        await self.scheduler.start()

    async def _stop(self, exception: Exception = None) -> None:
        await self.scheduler.stop()
        await self.scheduler.destroy()

    @cached_property
    def scheduler(self) -> PeriodicTaskScheduler:
        """Get the service scheduler.

        :return: A ``PeriodicTaskScheduler`` instance.
        """
        return PeriodicTaskScheduler.from_config(**self._init_kwargs)


class PeriodicTaskSchedulerService(PeriodicPort):
    """Periodic Task Scheduler Service class."""

    def __init__(self, **kwargs):
        warnings.warn(
            f"{PeriodicTaskSchedulerService!r} has been deprecated. Use {PeriodicPort} instead.",
            DeprecationWarning,
        )
        super().__init__(**kwargs)

import logging

from aiomisc import (
    Service,
)
from cached_property import (
    cached_property,
)

from .schedulers import (
    TaskScheduler,
)

logger = logging.getLogger(__name__)


class TaskSchedulerService(Service):
    """TODO"""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._init_kwargs = kwargs

    async def start(self) -> None:
        """Start the service execution.

        :return: This method does not return anything.
        """
        await self.scheduler.setup()

        try:
            self.start_event.set()
        except RuntimeError:
            logger.warning("Runtime is not properly setup.")

        await self.scheduler.start()

    async def stop(self, exception: Exception = None) -> None:
        """Stop the service execution.

        :param exception: Optional exception that stopped the execution.
        :return: This method does not return anything.
        """
        await self.scheduler.destroy()

    @cached_property
    def scheduler(self) -> TaskScheduler:
        """Get the service scheduler.

        :return: A ``TaskScheduler`` instance.
        """
        return TaskScheduler.from_config(**self._init_kwargs)

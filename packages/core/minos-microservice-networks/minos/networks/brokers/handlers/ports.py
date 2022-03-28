import logging
import warnings

from cached_property import (
    cached_property,
)

from minos.common import (
    Port,
)

from .impl import (
    BrokerHandler,
)

logger = logging.getLogger(__name__)


class BrokerPort(Port):
    """Broker Port class."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._init_kwargs = kwargs

    async def _start(self) -> None:
        await self.handler.setup()

        try:
            self.start_event.set()
        except RuntimeError:
            logger.warning("Runtime is not properly setup.")

        await self.handler.run()

    async def _stop(self, err: Exception = None) -> None:
        await self.handler.destroy()

    @cached_property
    def handler(self) -> BrokerHandler:
        """Get the service handler.

        :return: A ``Handler`` instance.
        """
        return BrokerHandler.from_config(**self._init_kwargs)


class BrokerHandlerService(BrokerPort):
    """Broker Handler Service class."""

    def __init__(self, **kwargs):
        warnings.warn(f"{BrokerHandlerService!r} has been deprecated. Use {BrokerPort} instead.", DeprecationWarning)
        super().__init__(**kwargs)

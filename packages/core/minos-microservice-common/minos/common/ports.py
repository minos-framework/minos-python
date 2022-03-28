import logging
from abc import (
    ABC,
    abstractmethod,
)

from aiomisc import (
    Service,
)

logger = logging.getLogger(__name__)


class Port(Service, ABC):
    """Port base class."""

    async def start(self) -> None:
        """Start the port execution.

        :return: This method does not return anything.
        """
        try:
            return await self._start()
        except Exception as exc:
            logger.exception(f"Raised an exception on {self!r} start: {exc!r}")
            raise exc

    @abstractmethod
    def _start(self):
        raise NotImplementedError

    async def stop(self, err: Exception = None) -> None:
        """Stop the port execution.

        :param err: Optional exception that stopped the execution.
        :return: This method does not return anything.
        """
        try:
            return await self._stop(err)
        except Exception as exc:
            logger.exception(f"Raised an exception on {self!r} stop: {exc!r}")
            raise exc

    @abstractmethod
    async def _stop(self, err: Exception = None) -> None:
        raise NotImplementedError

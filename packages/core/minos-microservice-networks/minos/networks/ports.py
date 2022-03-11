from abc import (
    ABC,
    abstractmethod,
)

from aiomisc import (
    Service,
)


class Port(Service, ABC):
    """Port base class."""

    @abstractmethod
    async def start(self) -> None:
        """Start the port execution.

        :return: This method does not return anything.
        """

    @abstractmethod
    async def stop(self, err: Exception = None) -> None:
        """Stop the port execution.

        :param err: Optional exception that stopped the execution.
        :return: This method does not return anything.
        """

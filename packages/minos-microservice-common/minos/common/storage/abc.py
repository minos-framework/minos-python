from __future__ import (
    annotations,
)

from abc import (
    ABC,
    abstractmethod,
)
from typing import (
    Any,
    Optional,
)


class MinosStorage(ABC):
    """Minos Storage interface."""

    @abstractmethod
    def add(self, **kwargs) -> None:
        """Store a value.

        :param kwargs: Additional named arguments.
        :return: This method does not return anything.
        """
        raise NotImplementedError

    @abstractmethod
    def get(self, **kwargs) -> Optional[Any]:
        """Get the stored value..

        :param kwargs: Additional named arguments.
        :return: The stored value.
        """
        raise NotImplementedError

    @abstractmethod
    def delete(self, **kwargs) -> None:
        """Delete the stored value.

        :param kwargs:
        :return: This method does not return anything.
        """
        raise NotImplementedError

    @abstractmethod
    def update(self, **kwargs) -> None:
        """Update the stored value.

        :param kwargs: Additional named arguments.
        :return: This method does not return anything.
        """
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def build(cls, **kwargs) -> MinosStorage:
        """Build a new instance.

        :param kwargs: Additional named arguments.
        :return: A new ``MinosStorage`` instance.
        """
        raise NotImplementedError

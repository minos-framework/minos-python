"""minos.common.snapshot.abc module."""
from __future__ import (
    annotations,
)

from abc import (
    ABC,
    abstractmethod,
)
from typing import (
    TYPE_CHECKING,
    AsyncIterator,
    Optional,
)
from uuid import (
    UUID,
)

from ..queries import (
    _Condition,
    _Ordering,
)
from ..setup import (
    MinosSetup,
)

if TYPE_CHECKING:
    from ..model import (
        Aggregate,
    )


class MinosSnapshot(ABC, MinosSetup):
    """Base Snapshot class."""

    @abstractmethod
    async def get(self, aggregate_name: str, uuid: UUID, **kwargs) -> Aggregate:
        """TODO

        :param aggregate_name: Class name of the ``Aggregate`` to be retrieved.
        :param uuid: Set of identifiers to be retrieved.
        :param kwargs: Additional named arguments.
        :return: An asynchronous iterator that provides the requested ``Aggregate`` instances.
        """

    @abstractmethod
    async def find(
        self,
        aggregate_name: str,
        condition: _Condition,
        ordering: Optional[_Ordering] = None,
        limit: Optional[int] = None,
        **kwargs
    ) -> AsyncIterator[Aggregate]:
        """TODO

        :param aggregate_name: Class name of the ``Aggregate`` to be retrieved.
        :param condition: TODO
        :param ordering: TODO
        :param limit: TODO
        :param kwargs: Additional named arguments.
        :return: An asynchronous iterator that provides the requested ``Aggregate`` instances.
        """

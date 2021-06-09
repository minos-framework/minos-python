"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
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
    async def get(self, aggregate_name: str, ids: list[int], **kwargs) -> AsyncIterator[Aggregate]:
        """Retrieves a list of  materialised ``Aggregate`` instances.

        :param aggregate_name: TODO
        :param ids: TODO
        :param kwargs: TODO
        :return: TODO
        """

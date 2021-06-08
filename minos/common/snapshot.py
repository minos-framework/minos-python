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
)

if TYPE_CHECKING:
    from .model import (
        Aggregate,
    )


class MinosSnapshot(ABC):
    """Base Snapshot class."""

    @abstractmethod
    async def get(self, ids: list[int], **kwargs) -> list[Aggregate]:
        """Retrieves a list of  materialised ``Aggregate`` instances."""

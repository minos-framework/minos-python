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
    NoReturn,
    Optional,
)

from .model import (
    CommandReply,
)


class MinosSagaManager(ABC):
    """Base class for saga manager implementations."""

    @abstractmethod
    def run(self, name: Optional[str] = None, reply: Optional[CommandReply] = None) -> NoReturn:
        """Performs a run

        :param name: The name of the saga to be executed.
        :param reply: The reply that relaunches a saga execution.
        :return: This method does not return anything.
        """

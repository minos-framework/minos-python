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

    def run(self, name: Optional[str] = None, reply: Optional[CommandReply] = None) -> NoReturn:
        """Perform a run of a ``Saga``.

        The run can be a new one (if a name is provided) or continue execution a previous one (if a reply is provided).

        :param name: The name of the saga to be executed.
        :param reply: The reply that relaunches a saga execution.
        :return: This method does not return anything.
        """
        if name is not None:
            return self._run_new(name)

        if reply is not None:
            return self._load_and_run(reply)

        raise ValueError("At least a 'name' or a 'reply' must be provided.")

    @abstractmethod
    def _run_new(self, name: str) -> NoReturn:
        raise NotImplementedError

    @abstractmethod
    def _load_and_run(self, reply: CommandReply) -> NoReturn:
        raise NotImplementedError

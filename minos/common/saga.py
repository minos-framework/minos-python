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
    Optional,
)
from uuid import (
    UUID,
)

from .model import (
    CommandReply,
)
from .setup import (
    MinosSetup,
)


class MinosSagaManager(ABC, MinosSetup):
    """Base class for saga manager implementations."""

    async def run(self, name: Optional[str] = None, reply: Optional[CommandReply] = None, **kwargs) -> UUID:
        """Perform a run of a ``Saga``.

        The run can be a new one (if a name is provided) or continue execution a previous one (if a reply is provided).

        :param name: The name of the saga to be executed.
        :param reply: The reply that relaunches a saga execution.
        :param kwargs: Additional named arguments.
        :return: This method does not return anything.
        """
        if name is not None:
            return await self._run_new(name, **kwargs)

        if reply is not None:
            return await self._load_and_run(reply, **kwargs)

        raise ValueError("At least a 'name' or a 'reply' must be provided.")

    @abstractmethod
    async def _run_new(self, name: str, **kwargs) -> UUID:
        raise NotImplementedError

    @abstractmethod
    async def _load_and_run(self, reply: CommandReply, **kwargs) -> UUID:
        raise NotImplementedError

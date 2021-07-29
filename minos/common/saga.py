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

    async def run(self, *args, reply: Optional[CommandReply] = None, **kwargs) -> UUID:
        """Perform a run of a ``Saga``.

        The run can be a new one (if a name is provided) or continue execution a previous one (if a reply is provided).

        :param reply: The reply that relaunches a saga execution.
        :param kwargs: Additional named arguments.
        :return: This method does not return anything.
        """

        if reply is not None:
            return await self._load_and_run(*args, reply, **kwargs)

        return await self._run_new(*args, **kwargs)

    @abstractmethod
    async def _run_new(self, *args, **kwargs) -> UUID:
        raise NotImplementedError

    @abstractmethod
    async def _load_and_run(self, *args, reply: CommandReply, **kwargs) -> UUID:
        raise NotImplementedError

"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import inspect
from abc import (
    ABC,
)
from typing import (
    Any,
    Callable,
)

from ...definitions import (
    SagaStepOperation,
)
from ..context import (
    SagaContext,
)


class LocalExecutor(ABC):
    """Local executor class."""

    def __init__(self, *args, **kwargs):
        pass

    async def exec_one(self, operation: SagaStepOperation, *args, **kwargs) -> Any:
        """Execute the given operation locally.

        :param operation: The operation to be executed.
        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: The execution response.
        """

        return await self._exec_function(operation.callback, *args, **kwargs)

    @staticmethod
    async def _exec_function(func: Callable, *args, **kwargs) -> SagaContext:
        result = func(*args, **kwargs)
        if inspect.isawaitable(result):
            result = await result
        return result

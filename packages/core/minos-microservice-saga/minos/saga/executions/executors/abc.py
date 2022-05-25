import inspect
import sys
from typing import (
    Any,
    Callable,
)
from uuid import (
    UUID,
)

from minos.transactions import (
    TransactionEntry,
)

from ...definitions import (
    SagaOperation,
)
from ...exceptions import (
    ExecutorException,
)


class Executor:
    """Executor class."""

    def __init__(self, execution_uuid: UUID, *args, **kwargs):
        self.execution_uuid = execution_uuid

    async def exec(self, operation: SagaOperation, *args, **kwargs) -> Any:
        """Execute the given operation.

        :param operation: The operation to be executed.
        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: The execution response.
        """

        if operation.parameterized:
            kwargs = {**operation.parameters, **kwargs}

        try:
            func = self._get_method(operation.callback)
        except Exception:  # FIXME
            func = operation.callback

        return await self.exec_function(func, *args, **kwargs)

    def _get_method(self, func) -> type:
        cls = self._get_cls(func)
        obj = cls()
        method = getattr(obj, func.__name__)
        return method

    @staticmethod
    def _get_cls(func):
        vals = vars(sys.modules[func.__module__])
        for attr in func.__qualname__.split(".")[:-1]:
            vals = vals[attr]
        return vals

    async def exec_function(self, func: Callable, *args, **kwargs) -> Any:
        """Execute a function.

        :param func: Function to be executed.
        :param args: Additional positional arguments to the function.
        :param kwargs: Additional named arguments to the function.
        :return: The ``func`` result.
        """
        try:
            async with TransactionEntry(uuid=self.execution_uuid, autocommit=False):
                result = func(*args, **kwargs)
                if inspect.isawaitable(result):
                    result = await result
        except Exception as exc:
            raise ExecutorException(exc)
        return result

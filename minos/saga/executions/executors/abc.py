import inspect
from typing import (
    Any,
    Callable,
)
from uuid import (
    UUID,
)

from minos.aggregate import (
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

        return await self.exec_function(operation.callback, *args, **kwargs)

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

from typing import (
    Optional,
)

from ...context import (
    SagaContext,
)
from ...definitions import (
    LocalCallback,
    SagaOperation,
)
from ...exceptions import (
    ExecutorException,
    SagaFailedExecutionStepException,
)
from .abc import (
    Executor,
)


class LocalExecutor(Executor):
    """Local Executor class."""

    # noinspection PyUnusedLocal,PyMethodOverriding
    async def exec(
        self, operation: Optional[SagaOperation[LocalCallback]], context: SagaContext, *args, **kwargs
    ) -> SagaContext:
        """Execute the commit operation.

        :param operation: Operation to be executed.
        :param context: Actual execution context.
        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: An updated context instance.
        """

        if operation is None:
            return context

        try:
            context = SagaContext(**context)  # Needed to avoid mutability issues.
            new_context = await super().exec(operation, context)
        except ExecutorException as exc:
            raise SagaFailedExecutionStepException(exc.exception)

        if new_context is not None:
            context = new_context

        return context

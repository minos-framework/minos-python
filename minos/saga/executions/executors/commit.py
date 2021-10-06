from typing import (
    Optional,
)

from ...context import (
    SagaContext,
)
from ...definitions import (
    SagaOperation,
)
from ...exceptions import (
    MinosSagaExecutorException,
    MinosSagaFailedCommitCallbackException,
)
from .local import (
    LocalExecutor,
)


class CommitExecutor(LocalExecutor):
    """Commit Executor class."""

    # noinspection PyUnusedLocal
    async def exec(self, operation: Optional[SagaOperation], context: SagaContext, *args, **kwargs) -> SagaContext:
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
            new_context = await self.exec_operation(operation, context)
        except MinosSagaExecutorException as exc:
            raise MinosSagaFailedCommitCallbackException(exc.exception)

        if new_context is not None:
            context = new_context

        return context

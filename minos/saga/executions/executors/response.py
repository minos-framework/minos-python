from typing import (
    Optional,
)

from minos.common import (
    CommandReply,
)

from ...context import (
    SagaContext,
)
from ...definitions import (
    ResponseCallBack,
    SagaOperation,
)
from ...exceptions import (
    ExecutorException,
    SagaFailedExecutionStepException,
)
from ...messages import (
    SagaResponse,
)
from .abc import (
    Executor,
)


class ResponseExecutor(Executor):
    """Response Executor class."""

    # noinspection PyUnusedLocal,PyMethodOverriding
    async def exec(
        self,
        operation: Optional[SagaOperation[ResponseCallBack]],
        context: SagaContext,
        reply: CommandReply,
        *args,
        **kwargs
    ) -> SagaContext:
        """Execute the operation.

        :param operation: Operation to be executed.
        :param context: Actual execution context.
        :param reply: Command Reply which contains the response.
        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: An updated context instance.
        """
        if operation is None:
            return context

        try:
            response = SagaResponse(reply.data, reply.status)
            context = SagaContext(**context)  # Needed to avoid mutability issues.
            context = await super().exec(operation, context, response)
        except ExecutorException as exc:
            raise SagaFailedExecutionStepException(exc.exception)

        if isinstance(context, Exception):
            raise SagaFailedExecutionStepException(context)

        return context

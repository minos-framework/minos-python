"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

from minos.common import (
    CommandReply,
)

from ...definitions import (
    SagaStepOperation,
)
from ...exceptions import (
    MinosSagaFailedExecutionStepException,
    MinosSagaPausedExecutionStepException,
)
from ..context import (
    SagaContext,
)
from .local import (
    LocalExecutor,
)


class OnReplyExecutor(LocalExecutor):
    """On Reply Executor class."""

    # noinspection PyUnusedLocal
    async def exec(
        self, operation: SagaStepOperation, context: SagaContext, reply: CommandReply, *args, **kwargs
    ) -> SagaContext:
        """Execute the on reply operation.

        :param operation: Operation to be executed.
        :param context: Actual execution context.
        :param reply: Command Reply which contains the response.
        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: An updated context instance.
        """
        if operation is None:
            return context

        if reply is None:
            raise MinosSagaPausedExecutionStepException()

        value = reply.items
        if len(value) == 1:
            value = value[0]

        try:
            response = await super().exec_one(operation, value)
            context[operation.name] = response
        except Exception as exc:
            raise MinosSagaFailedExecutionStepException(exc)

        return context

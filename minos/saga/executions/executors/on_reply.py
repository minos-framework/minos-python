"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

from minos.common import (
    Aggregate,
)

from ...definitions import (
    SagaStepOperation,
)
from ...exceptions import (
    MinosSagaPausedExecutionStepException,
)
from ..context import (
    SagaContext,
)
from .local import (
    LocalExecutor,
)


class OnReplyExecutor(LocalExecutor):
    """TODO"""

    # noinspection PyUnusedLocal
    def exec(self, operation: SagaStepOperation, context: SagaContext, response: Aggregate = None, *args, **kwargs):
        """TODO

        :param operation: TODO
        :param context: TODO
        :param response: TODO
        :param args: TODO
        :param kwargs: TODO
        :return: TODO
        """
        if operation is None:
            return context

        if response is None:
            raise MinosSagaPausedExecutionStepException()

        response = super().exec_one(operation, response)
        context.update(operation.name, response)
        return context

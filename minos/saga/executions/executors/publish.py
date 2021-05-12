"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from typing import (
    NoReturn,
)

from minos.common import (
    MinosModel,
)

from ...definitions import (
    SagaStepOperation,
)
from ...exceptions import (
    MinosSagaException,
    MinosSagaFailedExecutionStepException,
)
from ..context import (
    SagaContext,
)
from .local import (
    LocalExecutor,
)


class PublishExecutor(LocalExecutor):
    """Publish Executor class.

    This class has the responsibility to publish command on the corresponding broker's queue. """

    def exec(self, operation: SagaStepOperation, context: SagaContext) -> SagaContext:
        """Exec method, that perform the publishing logic run an pre-callback function to generate the command contents.

        :param operation: Operation to be executed.
        :param context: Execution context.
        :return: A saga context instance.
        """
        if operation is None:
            return context

        try:
            request = self.exec_one(operation, context)
            self.publish(request)
        except MinosSagaException as exc:
            raise exc
        except Exception:
            exc = MinosSagaFailedExecutionStepException()  # FIXME: Include explanation.
            raise exc

        return context

    @staticmethod
    def publish(request: MinosModel) -> NoReturn:
        """Publish a request on the corresponding broker's queue./

        :param request: The request to be published as a command.
        :return: This method does not return anything.
        """
        # FIXME: Publish the command

"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

from typing import (
    NoReturn,
)
from uuid import (
    UUID,
)

from dependency_injector.wiring import (
    Provide,
)

from minos.common import (
    MinosBroker,
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

    This class has the responsibility to publish command on the corresponding broker's queue.
    """

    broker: MinosBroker = Provide["command_broker"]

    def __init__(self, *args, definition_name: str, execution_uuid: UUID, broker: MinosBroker = None, **kwargs):
        super().__init__(*args, **kwargs)
        if broker is not None:
            self.broker = broker

        self.definition_name = definition_name
        self.execution_uuid = execution_uuid

    async def exec(self, operation: SagaStepOperation, context: SagaContext, on_reply: bool) -> SagaContext:
        """Exec method, that perform the publishing logic run an pre-callback function to generate the command contents.

        :param operation: Operation to be executed.
        :param context: Execution context.
        :param on_reply: TODO
        :return: A saga context instance.
        """
        if operation is None:
            return context

        try:
            request = await self.exec_one(operation, context)
            await self._publish(operation, request, on_reply)
        except MinosSagaException as exc:
            raise exc
        except Exception:
            exc = MinosSagaFailedExecutionStepException()  # FIXME: Include explanation.
            raise exc

        return context

    async def _publish(self, operation: SagaStepOperation, request: MinosModel, on_reply: bool) -> NoReturn:
        await self._exec_function(
            self.broker.send_one,
            topic=operation.name,
            item=request,
            saga_id=self.definition_name,
            task_id=str(self.execution_uuid),
            on_reply=on_reply,
        )

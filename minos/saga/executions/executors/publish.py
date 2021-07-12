"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

from typing import (
    Any,
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
)

from ... import (
    MinosSagaFailedExecutionStepException,
)
from ...definitions import (
    SagaStepOperation,
)
from ...exceptions import (
    MinosSagaExecutorException,
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

    async def exec(self, operation: SagaStepOperation, context: SagaContext) -> SagaContext:
        """Exec method, that perform the publishing logic run an pre-callback function to generate the command contents.

        :param operation: Operation to be executed.
        :param context: Execution context.
        :return: A saga context instance.
        """
        if operation is None:
            return context

        try:
            request = await self.exec_operation(operation, context)
            await self._publish(operation, request)
        except MinosSagaExecutorException as exc:
            raise MinosSagaFailedExecutionStepException(exc.exception)
        return context

    async def _publish(self, operation: SagaStepOperation, data: Any) -> NoReturn:
        topic = operation.name
        saga = self.execution_uuid
        reply_topic = self.definition_name
        await self.exec_function(self.broker.send, topic=topic, data=data, saga=saga, reply_topic=reply_topic)

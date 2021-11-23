from __future__ import (
    annotations,
)

from typing import (
    Optional,
)
from uuid import (
    UUID,
)

from dependency_injector.wiring import (
    Provide,
    inject,
)

from minos.common import (
    MinosConfig,
    NotProvidedException,
)
from minos.networks import (
    REPLY_TOPIC_CONTEXT_VAR,
    BrokerPublisher,
)

from ...context import (
    SagaContext,
)
from ...definitions import (
    RequestCallBack,
    SagaOperation,
)
from ...exceptions import (
    ExecutorException,
    SagaFailedExecutionStepException,
)
from ...messages import (
    SagaRequest,
)
from .abc import (
    Executor,
)


class RequestExecutor(Executor):
    """Request Executor class.

    This class has the responsibility to publish command on the corresponding broker's queue.
    """

    @inject
    def __init__(
        self, *args, user: Optional[UUID], broker_publisher: BrokerPublisher = Provide["broker_publisher"], **kwargs,
    ):
        super().__init__(*args, **kwargs)

        self.user = user

        if broker_publisher is None or isinstance(broker_publisher, Provide):
            raise NotProvidedException("A broker instance is required.")

        self.broker_publisher = broker_publisher

    # noinspection PyMethodOverriding
    async def exec(self, operation: Optional[SagaOperation[RequestCallBack]], context: SagaContext) -> SagaContext:
        """Exec method, that perform the publishing logic run an pre-callback function to generate the command contents.

        :param operation: Operation to be executed.
        :param context: Execution context.
        :return: A saga context instance.
        """
        if operation is None:
            return context

        try:
            context = SagaContext(**context)  # Needed to avoid mutability issues.
            request = await super().exec(operation, context)
            await self._publish(request)
        except ExecutorException as exc:
            raise SagaFailedExecutionStepException(exc.exception)
        return context

    # noinspection PyMethodOverriding
    async def _publish(self, request: SagaRequest) -> None:
        reply_topic = REPLY_TOPIC_CONTEXT_VAR.get()
        if reply_topic is None:
            reply_topic = self._get_default_reply_topic()

        try:
            await self.broker_publisher.send(
                topic=request.target,
                data=await request.content(),
                saga=self.execution_uuid,
                user=self.user,
                reply_topic=reply_topic,
            )
        except Exception as exc:
            raise ExecutorException(exc)

    @inject
    def _get_default_reply_topic(self, config: MinosConfig = Provide["config"]) -> str:
        return f"{config.service.name}Reply"

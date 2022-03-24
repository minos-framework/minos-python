from __future__ import (
    annotations,
)

from typing import (
    Optional,
)
from uuid import (
    UUID,
)

from minos.common import (
    Config,
    Inject,
    NotProvidedException,
)
from minos.networks import (
    REQUEST_HEADERS_CONTEXT_VAR,
    REQUEST_REPLY_TOPIC_CONTEXT_VAR,
    BrokerMessageV1,
    BrokerMessageV1Payload,
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

    @Inject()
    def __init__(self, *args, user: Optional[UUID], broker_publisher: BrokerPublisher, **kwargs):
        super().__init__(*args, **kwargs)

        self.user = user

        if broker_publisher is None:
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
        reply_topic = REQUEST_REPLY_TOPIC_CONTEXT_VAR.get()
        if reply_topic is None:
            reply_topic = self._get_default_reply_topic()

        headers = (REQUEST_HEADERS_CONTEXT_VAR.get() or dict()).copy()
        headers["saga"] = str(self.execution_uuid)
        if self.user is not None:
            headers["user"] = str(self.user)
        if headers.get("transactions"):
            headers["transactions"] += f",{self.execution_uuid!s}"
        else:
            headers["transactions"] = f"{self.execution_uuid!s}"

        try:
            message = BrokerMessageV1(
                topic=request.target,
                payload=BrokerMessageV1Payload(content=await request.content(), headers=headers),
                reply_topic=reply_topic,
            )

            await self.broker_publisher.send(message)
        except Exception as exc:
            raise ExecutorException(exc)

    @Inject()
    def _get_default_reply_topic(self, config: Config) -> str:
        return f"{config.get_name()}Reply"

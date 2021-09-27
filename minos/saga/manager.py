from __future__ import (
    annotations,
)

import logging
from typing import (
    Optional,
    Union,
)
from uuid import (
    UUID,
)

from dependency_injector.wiring import (
    Provide,
    inject,
)

from minos.common import (
    CommandReply,
    MinosConfig,
    MinosHandler,
    MinosHandlerNotProvidedException,
    MinosPool,
    MinosSagaManager,
)

from .context import (
    SagaContext,
)
from .definitions import (
    Saga,
)
from .exceptions import (
    MinosSagaFailedExecutionException,
    MinosSagaPausedExecutionStepException,
)
from .executions import (
    SagaExecution,
    SagaExecutionStorage,
    SagaStatus,
)

logger = logging.getLogger(__name__)


class SagaManager(MinosSagaManager[Union[SagaExecution, UUID]]):
    """Saga Manager implementation class.

    The purpose of this class is to manage the running process for new or paused``SagaExecution`` instances.
    """

    @inject
    def __init__(
        self,
        storage: SagaExecutionStorage,
        dynamic_handler_pool: MinosPool[MinosHandler] = Provide["dynamic_handler_pool"],
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.storage = storage

        if dynamic_handler_pool is None or isinstance(dynamic_handler_pool, Provide):
            raise MinosHandlerNotProvidedException("A handler pool instance is required.")

        self.dynamic_handler_pool = dynamic_handler_pool

    @classmethod
    def _from_config(cls, *args, config: MinosConfig, **kwargs) -> SagaManager:
        """Build an instance from config.

        :param args: Additional positional arguments.
        :param config: Config instance.
        :param kwargs: Additional named arguments.
        :return: A new ``SagaManager`` instance.
        """
        storage = SagaExecutionStorage.from_config(config=config, **kwargs)
        return cls(*args, storage=storage, **kwargs)

    async def _run_new(
        self, definition: Saga, context: Optional[SagaContext] = None, **kwargs,
    ) -> Union[UUID, SagaExecution]:
        execution = SagaExecution.from_saga(definition, context=context)
        return await self._run(execution, **kwargs)

    async def _load_and_run(self, reply: CommandReply, **kwargs) -> Union[UUID, SagaExecution]:
        execution = self.storage.load(reply.saga)
        return await self._run(execution, reply=reply, **kwargs)

    async def _run(
        self,
        execution: SagaExecution,
        pause_on_disk: bool = False,
        raise_on_error: bool = True,
        return_execution: bool = True,
        **kwargs,
    ) -> Union[UUID, SagaExecution]:
        try:
            if pause_on_disk:
                await self._run_with_pause_on_disk(execution, **kwargs)
            else:
                await self._run_with_pause_on_memory(execution, **kwargs)
        except MinosSagaFailedExecutionException as exc:
            self.storage.store(execution)
            if raise_on_error:
                raise exc
            logger.warning(f"The execution identified by {execution.uuid!s} failed: {exc.exception!r}")

        if execution.status == SagaStatus.Finished:
            self.storage.delete(execution)

        if return_execution:
            return execution

        return execution.uuid

    async def _run_with_pause_on_disk(self, execution: SagaExecution, **kwargs) -> None:
        try:
            await execution.execute(**kwargs)
        except MinosSagaPausedExecutionStepException:
            self.storage.store(execution)

    async def _run_with_pause_on_memory(
        self, execution: SagaExecution, reply: Optional[CommandReply] = None, **kwargs
    ) -> None:

        # noinspection PyUnresolvedReferences
        async with self.dynamic_handler_pool.acquire() as handler:
            while execution.status in (SagaStatus.Created, SagaStatus.Paused):
                try:
                    await execution.execute(reply=reply, **(kwargs | {"reply_topic": handler.topic}))
                except MinosSagaPausedExecutionStepException:
                    reply = await self._get_reply(handler, execution, **kwargs)
                self.storage.store(execution)

    @staticmethod
    async def _get_reply(handler: MinosHandler, execution: SagaExecution, **kwargs) -> CommandReply:
        reply = None
        while reply is None or reply.saga != execution.uuid:
            try:
                entry = await handler.get_one(**kwargs)
            except Exception as exc:
                execution.status = SagaStatus.Errored
                raise MinosSagaFailedExecutionException(exc)
            reply = entry.data
        return reply

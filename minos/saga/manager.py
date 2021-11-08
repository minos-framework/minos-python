from __future__ import (
    annotations,
)

import logging
import warnings
from contextvars import (
    ContextVar,
    copy_context,
)
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
    MinosPool,
    MinosSagaManager,
    NotProvidedException,
)

from .context import (
    SagaContext,
)
from .definitions import (
    Saga,
)
from .exceptions import (
    SagaFailedExecutionException,
    SagaPausedExecutionStepException,
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
        user_context_var: Optional[ContextVar[Optional[UUID]]] = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.storage = storage

        if dynamic_handler_pool is None or isinstance(dynamic_handler_pool, Provide):
            raise NotProvidedException("A handler pool instance is required.")

        self.dynamic_handler_pool = dynamic_handler_pool
        self.user_context_var = user_context_var

    @classmethod
    def _from_config(cls, *args, config: MinosConfig, **kwargs) -> SagaManager:
        """Build an instance from config.

        :param args: Additional positional arguments.
        :param config: Config instance.
        :param kwargs: Additional named arguments.
        :return: A new ``SagaManager`` instance.
        """
        user_context_var = cls._user_context_var_from_config(config)
        storage = SagaExecutionStorage.from_config(config, **kwargs)
        return cls(storage=storage, user_context_var=user_context_var, **kwargs)

    @staticmethod
    def _user_context_var_from_config(*args, **kwargs) -> Optional[ContextVar]:
        context = copy_context()
        for var in context:
            if var.name == "user":
                return var
        return None

    async def _run_new(
        self, definition: Saga, context: Optional[SagaContext] = None, user: Optional[UUID] = None, **kwargs,
    ) -> Union[UUID, SagaExecution]:
        if self.user_context_var is not None:
            if user is not None:
                warnings.warn("The `user` Argument will be ignored in favor of the `user` ContextVar", RuntimeWarning)
            user = self.user_context_var.get()

        execution = SagaExecution.from_definition(definition, context=context, user=user)
        return await self._run(execution, **kwargs)

    async def _load_and_run(
        self, reply: CommandReply, user: Optional[UUID] = None, **kwargs
    ) -> Union[UUID, SagaExecution]:
        # NOTE: ``user`` is consumed here to avoid its injection on already started sagas.

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
        except SagaFailedExecutionException as exc:
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
        except SagaPausedExecutionStepException:
            self.storage.store(execution)

    async def _run_with_pause_on_memory(
        self, execution: SagaExecution, reply: Optional[CommandReply] = None, **kwargs
    ) -> None:

        # noinspection PyUnresolvedReferences
        async with self.dynamic_handler_pool.acquire() as handler:
            while execution.status in (SagaStatus.Created, SagaStatus.Paused):
                try:
                    await execution.execute(reply=reply, **(kwargs | {"reply_topic": handler.topic}))
                except SagaPausedExecutionStepException:
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
                raise SagaFailedExecutionException(exc)
            reply = entry.data
        return reply

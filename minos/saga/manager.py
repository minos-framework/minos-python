from __future__ import (
    annotations,
)

import logging
import warnings
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
    MinosConfig,
    MinosSetup,
    NotProvidedException,
)
from minos.networks import (
    USER_CONTEXT_VAR,
    CommandReply,
    DynamicHandler,
    DynamicHandlerPool,
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
from .messages import (
    SagaResponse,
)

logger = logging.getLogger(__name__)


class SagaManager(MinosSetup):
    """Saga Manager implementation class.

    The purpose of this class is to manage the running process for new or paused``SagaExecution`` instances.
    """

    @inject
    def __init__(
        self,
        storage: SagaExecutionStorage,
        dynamic_handler_pool: DynamicHandlerPool = Provide["dynamic_handler_pool"],
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.storage = storage

        if dynamic_handler_pool is None or isinstance(dynamic_handler_pool, Provide):
            raise NotProvidedException("A handler pool instance is required.")

        self.dynamic_handler_pool = dynamic_handler_pool

    @classmethod
    def _from_config(cls, *args, config: MinosConfig, **kwargs) -> SagaManager:
        """Build an instance from config.

        :param args: Additional positional arguments.
        :param config: Config instance.
        :param kwargs: Additional named arguments.
        :return: A new ``SagaManager`` instance.
        """
        storage = SagaExecutionStorage.from_config(config, **kwargs)
        return cls(storage=storage, **kwargs)

    async def run(self, *args, response: Optional[SagaResponse] = None, **kwargs) -> Union[UUID, SagaExecution]:
        """Perform a run of a ``Saga``.

        The run can be a new one (if a name is provided) or continue execution a previous one (if a reply is provided).

        :param response: The reply that relaunches a saga execution.
        :param kwargs: Additional named arguments.
        :return: This method does not return anything.
        """

        if response is not None:
            return await self._load_and_run(*args, response, **kwargs)

        return await self._run_new(*args, **kwargs)

    async def _run_new(
        self, definition: Saga, context: Optional[SagaContext] = None, user: Optional[UUID] = None, **kwargs,
    ) -> Union[UUID, SagaExecution]:
        if USER_CONTEXT_VAR.get() is not None:
            if user is not None:
                warnings.warn("The `user` Argument will be ignored in favor of the `user` ContextVar", RuntimeWarning)
            user = USER_CONTEXT_VAR.get()

        execution = SagaExecution.from_definition(definition, context=context, user=user)
        return await self._run(execution, **kwargs)

    # noinspection PyUnusedLocal
    async def _load_and_run(
        self, response: SagaResponse, user: Optional[UUID] = None, **kwargs
    ) -> Union[UUID, SagaExecution]:
        # NOTE: ``user`` is consumed here to avoid its injection on already started sagas.

        execution = self.storage.load(response.uuid)
        return await self._run(execution, response=response, **kwargs)

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
        self, execution: SagaExecution, response: Optional[SagaResponse] = None, **kwargs
    ) -> None:

        # noinspection PyUnresolvedReferences
        async with self.dynamic_handler_pool.acquire() as handler:
            while execution.status in (SagaStatus.Created, SagaStatus.Paused):
                try:
                    await execution.execute(response=response, **kwargs)
                except SagaPausedExecutionStepException:
                    response = await self._get_response(handler, execution, **kwargs)
                self.storage.store(execution)

    @staticmethod
    async def _get_response(handler: DynamicHandler, execution: SagaExecution, **kwargs) -> SagaResponse:
        reply: Optional[CommandReply] = None
        while reply is None or reply.saga != execution.uuid:
            try:
                entry = await handler.get_one(**kwargs)
            except Exception as exc:
                execution.status = SagaStatus.Errored
                raise SagaFailedExecutionException(exc)
            reply = entry.data

        response = SagaResponse(reply.data, reply.status, reply.service_name)

        return response

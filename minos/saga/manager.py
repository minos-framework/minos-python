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
    DynamicBroker,
    DynamicBrokerPool,
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
        self, storage: SagaExecutionStorage, broker_pool: DynamicBrokerPool = Provide["broker_pool"], *args, **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.storage = storage

        if broker_pool is None or isinstance(broker_pool, Provide):
            raise NotProvidedException("A handler pool instance is required.")

        self.broker_pool = broker_pool

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

    async def run(
        self,
        definition: Optional[Saga] = None,
        context: Optional[SagaContext] = None,
        *,
        response: Optional[SagaResponse] = None,
        user: Optional[UUID] = None,
        autocommit: bool = True,
        pause_on_disk: bool = False,
        raise_on_error: bool = True,
        return_execution: bool = True,
        **kwargs,
    ) -> Union[UUID, SagaExecution]:
        """Perform a run of a ``Saga``.

        The run can be a new one (if a name is provided) or continue execution a previous one (if a reply is provided).

        :param definition: Saga definition to be executed.
        :param context: Initial context to be used during the execution. (Only used for new executions)
        :param response: The reply that relaunches a saga execution.
        :param user: The user identifier to be injected on remote steps.
        :param autocommit: If ``True`` the transactions are committed/rejected automatically. Otherwise, the ``commit``
            or ``reject`` must to be called manually.
        :param pause_on_disk: If ``True`` the pauses until remote steps' responses are paused on disk (background,
            non-blocking the execution). Otherwise, the pauses are waited on memory (online, blocking the execution)
        :param raise_on_error: If ``True`` exceptions are raised on error. Otherwise, the execution is returned normally
            but with ``Errored`` status.
        :param return_execution: If ``True`` the ``SagaExecution`` instance is returned. Otherwise, only the
            identifier (``UUID``) is returned.
        :param kwargs: Additional named arguments.
        :return: This method does not return anything.
        """

        if response is not None:
            return await self._load_and_run(
                response=response,
                autocommit=autocommit,
                pause_on_disk=pause_on_disk,
                raise_on_error=raise_on_error,
                return_execution=return_execution,
                **kwargs,
            )

        return await self._run_new(
            definition=definition,
            context=context,
            user=user,
            autocommit=autocommit,
            pause_on_disk=pause_on_disk,
            raise_on_error=raise_on_error,
            return_execution=return_execution,
            **kwargs,
        )

    async def _run_new(
        self, definition: Saga, context: Optional[SagaContext] = None, user: Optional[UUID] = None, **kwargs,
    ) -> Union[UUID, SagaExecution]:
        if USER_CONTEXT_VAR.get() is not None:
            if user is not None:
                warnings.warn("The `user` Argument will be ignored in favor of the `user` ContextVar", RuntimeWarning)
            user = USER_CONTEXT_VAR.get()

        execution = SagaExecution.from_definition(definition, context=context, user=user)
        return await self._run(execution, **kwargs)

    async def _load_and_run(self, response: SagaResponse, **kwargs) -> Union[UUID, SagaExecution]:
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

    async def _run_with_pause_on_disk(self, execution: SagaExecution, autocommit: bool = True, **kwargs) -> None:
        try:
            await execution.execute(autocommit=False, **kwargs)
            if autocommit:
                await execution.commit(**kwargs)
        except SagaPausedExecutionStepException:
            self.storage.store(execution)
        except SagaFailedExecutionException as exc:
            if autocommit:
                await execution.reject(**kwargs)
            raise exc

    async def _run_with_pause_on_memory(
        self, execution: SagaExecution, response: Optional[SagaResponse] = None, autocommit: bool = True, **kwargs
    ) -> None:

        try:
            # noinspection PyUnresolvedReferences
            async with self.broker_pool.acquire() as broker:
                while execution.status in (SagaStatus.Created, SagaStatus.Paused):
                    try:
                        await execution.execute(response=response, autocommit=False, **kwargs)
                    except SagaPausedExecutionStepException:
                        response = await self._get_response(broker, execution, **kwargs)
                    self.storage.store(execution)
            if autocommit:
                await execution.commit(**kwargs)
        except SagaFailedExecutionException as exc:
            if autocommit:
                await execution.reject(**kwargs)
            raise exc

    @staticmethod
    async def _get_response(handler: DynamicBroker, execution: SagaExecution, **kwargs) -> SagaResponse:
        reply = None
        while reply is None or reply.saga != execution.uuid:
            try:
                entry = await handler.get_one(**kwargs)
            except Exception as exc:
                execution.status = SagaStatus.Errored
                raise SagaFailedExecutionException(exc)
            reply = entry.data

        response = SagaResponse(reply.data, reply.status, reply.service_name)

        return response

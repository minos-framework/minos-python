from __future__ import (
    annotations,
)

import logging
import warnings
from functools import (
    reduce,
)
from operator import (
    or_,
)
from typing import (
    Optional,
    Union,
)
from uuid import (
    UUID,
)

from minos.common import (
    Inject,
    NotProvidedException,
    PoolFactory,
    SetupMixin,
)
from minos.networks import (
    REQUEST_HEADERS_CONTEXT_VAR,
    REQUEST_USER_CONTEXT_VAR,
    BrokerClient,
    BrokerClientPool,
    BrokerMessage,
)

from ..context import (
    SagaContext,
)
from ..definitions import (
    Saga,
    SagaDecoratorWrapper,
)
from ..exceptions import (
    SagaFailedExecutionException,
    SagaPausedExecutionStepException,
)
from ..messages import (
    SagaResponse,
)
from .repositories import (
    SagaExecutionRepository,
)
from .saga import (
    SagaExecution,
    SagaStatus,
)

logger = logging.getLogger(__name__)


class SagaRunner(SetupMixin):
    """TODO"""

    @Inject()
    def __init__(
        self,
        storage: SagaExecutionRepository,
        broker_pool: Optional[BrokerClientPool] = None,
        pool_factory: Optional[PoolFactory] = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        if storage is None:
            raise NotProvidedException(f"A {SagaExecutionRepository!r} instance is required.")

        if broker_pool is None and pool_factory is not None:
            broker_pool = pool_factory.get_pool("broker")

        if broker_pool is None:
            raise NotProvidedException(f"A {BrokerClientPool!r} instance is required.")

        self.storage = storage
        self.broker_pool = broker_pool

    async def run(
        self,
        definition: Optional[Union[Saga, SagaDecoratorWrapper]] = None,
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
            or ``reject`` must be called manually.
        :param pause_on_disk: If ``True`` the pauses until remote steps' responses are paused on disk (background,
            non-blocking the execution). Otherwise, the pauses are waited on memory (online, blocking the execution)
        :param raise_on_error: If ``True`` exceptions are raised on error. Otherwise, the execution is returned normally
            but with ``Errored`` status.
        :param return_execution: If ``True`` the ``SagaExecution`` instance is returned. Otherwise, only the
            identifier (``UUID``) is returned.
        :param kwargs: Additional named arguments.
        :return: This method does not return anything.
        """
        if isinstance(definition, SagaDecoratorWrapper):
            definition = definition.meta.definition

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
        self, definition: Saga, context: Optional[SagaContext] = None, user: Optional[UUID] = None, **kwargs
    ) -> Union[UUID, SagaExecution]:
        if REQUEST_USER_CONTEXT_VAR.get() is not None:
            if user is not None:
                warnings.warn("The `user` Argument will be ignored in favor of the `user` ContextVar", RuntimeWarning)
            user = REQUEST_USER_CONTEXT_VAR.get()

        execution = SagaExecution.from_definition(definition, context=context, user=user)
        return await self._run(execution, **kwargs)

    async def _load_and_run(self, response: SagaResponse, **kwargs) -> Union[UUID, SagaExecution]:
        execution = await self.storage.load(response.uuid)
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
            if raise_on_error:
                raise exc
            logger.exception(f"The execution identified by {execution.uuid!s} failed")
        finally:
            await self.storage.store(execution)
            self._update_request_headers(execution)

        if return_execution:
            return execution

        return execution.uuid

    @staticmethod
    def _update_request_headers(execution: SagaExecution) -> None:
        if (headers := REQUEST_HEADERS_CONTEXT_VAR.get()) is None:
            return

        related_services = reduce(or_, (s.related_services for s in execution.executed_steps), set())
        if execution.paused_step is not None:
            related_services.update(execution.paused_step.related_services)

        if raw_related_services := headers.get("related_services"):
            related_services.update(raw_related_services.split(","))

        headers["related_services"] = ",".join(related_services)

    @staticmethod
    async def _run_with_pause_on_disk(execution: SagaExecution, autocommit: bool = True, **kwargs) -> None:
        try:
            await execution.execute(autocommit=False, **kwargs)
            if autocommit:
                await execution.commit(**kwargs)
        except SagaPausedExecutionStepException:
            return
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
                        await self.storage.store(execution)
                        response = await self._get_response(broker, execution, **kwargs)
            if autocommit:
                await execution.commit(**kwargs)
        except SagaFailedExecutionException as exc:
            if autocommit:
                await execution.reject(**kwargs)
            raise exc

    @staticmethod
    async def _get_response(broker: BrokerClient, execution: SagaExecution, **kwargs) -> SagaResponse:
        message: Optional[BrokerMessage] = None
        while message is None or "saga" not in message.headers or UUID(message.headers["saga"]) != execution.uuid:
            try:
                message = await broker.receive(**kwargs)
            except Exception as exc:
                execution.status = SagaStatus.Errored
                raise SagaFailedExecutionException(exc)
        return SagaResponse.from_message(message)
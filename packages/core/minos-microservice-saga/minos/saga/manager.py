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
    Config,
    Inject,
    Injectable,
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
    DatabaseSagaExecutionRepository,
    SagaExecution,
    SagaExecutionRepository,
    SagaStatus,
)
from .messages import (
    SagaResponse,
)

logger = logging.getLogger(__name__)


@Injectable("saga_manager")
class SagaManager(SetupMixin):
    """Saga Manager implementation class.

    The purpose of this class is to manage the running process for new or paused``SagaExecution`` instances.
    """

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
        self.storage = storage

        if broker_pool is None and pool_factory is not None:
            broker_pool = pool_factory.get_pool("broker")

        if broker_pool is None:
            raise NotProvidedException(f"A {BrokerClientPool!r} instance is required.")

        self.broker_pool = broker_pool

    @classmethod
    def _from_config(cls, config: Config, **kwargs) -> SagaManager:
        """Build an instance from config.

        :param args: Additional positional arguments.
        :param config: Config instance.
        :param kwargs: Additional named arguments.
        :return: A new ``SagaManager`` instance.
        """
        storage = DatabaseSagaExecutionRepository.from_config(config, **kwargs)
        return cls(storage=storage, **kwargs)

    async def _setup(self) -> None:
        await super()._setup()
        await self.storage.setup()

    async def _destroy(self) -> None:
        await self.storage.destroy()
        await super()._destroy()

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
            await self.storage.store(execution)
            if raise_on_error:
                raise exc
            logger.exception(f"The execution identified by {execution.uuid!s} failed")
        finally:
            if (headers := REQUEST_HEADERS_CONTEXT_VAR.get()) is not None:
                related_services = reduce(or_, (s.related_services for s in execution.executed_steps), set())
                if execution.paused_step is not None:
                    related_services.update(execution.paused_step.related_services)

                if raw_related_services := headers.get("related_services"):
                    related_services.update(raw_related_services.split(","))

                headers["related_services"] = ",".join(related_services)

        if execution.status == SagaStatus.Finished:
            await self.storage.delete(execution)

        if return_execution:
            return execution

        return execution.uuid

    async def _run_with_pause_on_disk(self, execution: SagaExecution, autocommit: bool = True, **kwargs) -> None:
        try:
            await execution.execute(autocommit=False, **kwargs)
            if autocommit:
                await execution.commit(**kwargs)
        except SagaPausedExecutionStepException:
            await self.storage.store(execution)
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
                    await self.storage.store(execution)
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

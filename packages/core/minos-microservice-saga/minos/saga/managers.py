"""Managers module."""

from __future__ import (
    annotations,
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
    SetupMixin,
)

from .context import (
    SagaContext,
)
from .definitions import (
    Saga,
    SagaDecoratorWrapper,
)
from .executions import (
    DatabaseSagaExecutionRepository,
    SagaExecution,
    SagaExecutionRepository,
    SagaRunner,
)
from .messages import (
    SagaResponse,
)


@Injectable("saga_manager")
class SagaManager(SetupMixin):
    """Saga Manager implementation class.

    The purpose of this class is to manage the running process for new or paused``SagaExecution`` instances.
    """

    @Inject()
    def __init__(
        self,
        storage: SagaExecutionRepository,
        runner: SagaRunner,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.storage = storage
        self.runner = runner

    @classmethod
    def _from_config(cls, config: Config, **kwargs) -> SagaManager:
        """Build an instance from config.

        :param args: Additional positional arguments.
        :param config: Config instance.
        :param kwargs: Additional named arguments.
        :return: A new ``SagaManager`` instance.
        """
        if "storage" not in kwargs:
            kwargs["storage"] = DatabaseSagaExecutionRepository.from_config(config, **kwargs)
        if "runner" not in kwargs:
            kwargs["runner"] = SagaRunner.from_config(config, **kwargs)
        # noinspection PyTypeChecker
        return super()._from_config(config, **kwargs)

    async def _setup(self) -> None:
        await super()._setup()
        await self.storage.setup()
        await self.runner.setup()

    async def _destroy(self) -> None:
        await self.runner.destroy()
        await self.storage.destroy()
        await super()._destroy()

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
        return await self.runner.run(
            definition=definition,
            context=context,
            response=response,
            user=user,
            autocommit=autocommit,
            pause_on_disk=pause_on_disk,
            raise_on_error=raise_on_error,
            return_execution=return_execution,
            **kwargs,
        )

    async def get(self, uuid: UUID) -> SagaExecution:
        """Get a saga execution by identifier.

        :param uuid: The identifier of the execution..
        :return: A ``SagaExecution`` instance.
        """
        return await self.storage.load(uuid)

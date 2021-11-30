from __future__ import (
    annotations,
)

from typing import (
    Optional,
)

from ...context import (
    SagaContext,
)
from ...definitions import (
    RemoteSagaStep,
)
from ...exceptions import (
    SagaFailedExecutionStepException,
    SagaPausedExecutionStepException,
    SagaResponseException,
    SagaRollbackExecutionStepException,
)
from ...messages import (
    SagaResponse,
    SagaResponseStatus,
)
from ...utils import (
    get_service_name,
)
from ..executors import (
    RequestExecutor,
    ResponseExecutor,
)
from ..status import (
    SagaStepStatus,
)
from .abc import (
    SagaStepExecution,
)


class RemoteSagaStepExecution(SagaStepExecution):
    """Saga Execution Step class."""

    definition: RemoteSagaStep

    async def execute(
        self, context: SagaContext, response: Optional[SagaResponse] = None, *args, **kwargs
    ) -> SagaContext:
        """Execution the remote step.

        :param context: The execution context to be used during the execution.
        :param response: An optional command response instance (to be consumed by the on_success method).
        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: The updated context.
        """

        await self._execute_on_execute(context, *args, **kwargs)

        if response is None:
            self.status = SagaStepStatus.PausedByOnExecute
            raise SagaPausedExecutionStepException()

        self.related_services |= response.related_services

        if response.status == SagaResponseStatus.SYSTEM_ERROR:
            self.status = SagaStepStatus.ErroredByOnExecute
            exc = SagaResponseException(f"Failed with {response.status!s} status: {await response.content()!s}")
            raise SagaFailedExecutionStepException(exc)

        if response.status == SagaResponseStatus.SUCCESS:
            context = await self._execute_on_success(context, response, *args, **kwargs)
        else:
            context = await self._execute_on_error(context, response, *args, **kwargs)

        self.status = SagaStepStatus.Finished
        return context

    async def _execute_on_execute(self, context: SagaContext, *args, **kwargs) -> None:
        if self.status != SagaStepStatus.Created:
            return

        self.status = SagaStepStatus.RunningOnExecute
        executor = RequestExecutor(*args, **kwargs)
        self.related_services.add(get_service_name())
        try:
            await executor.exec(self.definition.on_execute_operation, context)
        except SagaFailedExecutionStepException as exc:
            self.status = SagaStepStatus.ErroredOnExecute
            raise exc
        self.status = SagaStepStatus.FinishedOnExecute

    async def _execute_on_success(self, context: SagaContext, response: SagaResponse, *args, **kwargs) -> SagaContext:
        self.status = SagaStepStatus.RunningOnSuccess
        executor = ResponseExecutor(*args, **kwargs)
        self.related_services.add(get_service_name())

        try:
            context = await executor.exec(self.definition.on_success_operation, context, response)
        except SagaFailedExecutionStepException as exc:
            self.status = SagaStepStatus.ErroredOnSuccess
            await self.rollback(context, *args, **kwargs)
            raise exc

        return context

    async def _execute_on_error(self, context: SagaContext, response: SagaResponse, *args, **kwargs) -> SagaContext:
        self.status = SagaStepStatus.RunningOnError
        executor = ResponseExecutor(*args, **kwargs)
        self.related_services.add(get_service_name())

        try:
            context = await executor.exec(self.definition.on_error_operation, context, response)
        except SagaFailedExecutionStepException as exc:
            self.status = SagaStepStatus.ErroredOnError
            await self.rollback(context, *args, **kwargs)
            raise exc

        return context

    async def rollback(self, context: SagaContext, *args, **kwargs) -> SagaContext:
        """Revert the executed remote step.

        :param context: Execution context.
        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: The updated execution context.
        """
        if self.status == SagaStepStatus.Created:
            raise SagaRollbackExecutionStepException("There is nothing to rollback.")

        if self.already_rollback:
            raise SagaRollbackExecutionStepException("The step was already rollbacked.")

        executor = RequestExecutor(*args, **kwargs)
        await executor.exec(self.definition.on_failure_operation, context)

        self.already_rollback = True
        return context

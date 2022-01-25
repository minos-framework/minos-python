from ...context import (
    SagaContext,
)
from ...definitions import (
    LocalSagaStep,
)
from ...exceptions import (
    SagaFailedExecutionStepException,
    SagaRollbackExecutionStepException,
)
from ...utils import (
    get_service_name,
)
from ..executors import (
    LocalExecutor,
)
from ..status import (
    SagaStepStatus,
)
from .abc import (
    SagaStepExecution,
)


class LocalSagaStepExecution(SagaStepExecution):
    """Local Saga Step Execution class."""

    definition: LocalSagaStep

    async def execute(self, context: SagaContext, *args, **kwargs) -> SagaContext:
        """Execute the local saga step.

        :param context: The execution context.
        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: The new saga context.
        """

        if self.status != SagaStepStatus.Created:
            return context

        self.status = SagaStepStatus.RunningOnExecute

        executor = LocalExecutor(*args, **kwargs)

        self.related_services.add(get_service_name())
        try:
            context = await executor.exec(self.definition.on_execute_operation, context)
        except SagaFailedExecutionStepException as exc:
            # await self.rollback(*args, **kwargs)  # Rollback must not be performed at this point.
            self.status = SagaStepStatus.ErroredOnExecute
            raise exc

        self.status = SagaStepStatus.Finished
        return context

    async def rollback(self, context: SagaContext, *args, **kwargs) -> SagaContext:
        """Rollback the local saga context.

        :param context: The execution context.
        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: The new saga context.
        """

        if self.status == SagaStepStatus.Created:
            raise SagaRollbackExecutionStepException("There is nothing to rollback.")

        if self.already_rollback:
            raise SagaRollbackExecutionStepException("The step was already rollbacked.")

        executor = LocalExecutor(*args, **kwargs)
        self.related_services.add(get_service_name())
        context = await executor.exec(self.definition.on_failure_operation, context)

        self.already_rollback = True
        return context

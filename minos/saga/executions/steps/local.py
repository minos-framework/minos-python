from ...context import (
    SagaContext,
)
from ...exceptions import (
    SagaFailedCommitCallbackException,
    SagaFailedExecutionStepException,
    SagaRollbackExecutionStepException,
)
from ..executors import (
    CommitExecutor,
)
from ..status import (
    SagaStepStatus,
)
from .abc import (
    SagaStepExecution,
)


class LocalSagaStepExecution(SagaStepExecution):
    """TODO"""

    async def execute(self, context: SagaContext, *args, **kwargs) -> SagaContext:
        """TODO"""

        if self.status != SagaStepStatus.Created:
            return context

        self.status = SagaStepStatus.RunningOnExecute

        executor = CommitExecutor(*args, **kwargs)

        try:
            context = await executor.exec(self.definition.on_execute_operation, context)
        except SagaFailedCommitCallbackException as exc:
            # await self.rollback(*args, **kwargs)  # Rollback must not be performed at this point.
            self.status = SagaStepStatus.ErroredOnExecute
            raise SagaFailedExecutionStepException(exc.exception)

        self.status = SagaStepStatus.Finished
        return context

    async def rollback(self, context: SagaContext, *args, **kwargs) -> SagaContext:
        """TODO"""

        if self.status == SagaStepStatus.Created:
            raise SagaRollbackExecutionStepException("There is nothing to rollback.")

        if self.already_rollback:
            raise SagaRollbackExecutionStepException("The step was already rollbacked.")

        executor = CommitExecutor(*args, **kwargs)
        context = await executor.exec(self.definition.on_failure_operation, context)

        self.already_rollback = True
        return context

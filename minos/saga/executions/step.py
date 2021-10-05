from __future__ import (
    annotations,
)

from typing import (
    Any,
    Iterable,
    Optional,
    Union,
)

from minos.common import (
    CommandReply,
)

from ..context import (
    SagaContext,
)
from ..definitions import (
    SagaStep,
)
from ..exceptions import (
    MinosCommandReplyFailedException,
    MinosSagaFailedExecutionStepException,
    MinosSagaPausedExecutionStepException,
    MinosSagaRollbackExecutionStepException,
)
from .executors import (
    RequestExecutor,
    ResponseExecutor,
)
from .status import (
    SagaStepStatus,
)


class SagaStepExecution:
    """Saga Execution Step class."""

    def __init__(
        self, definition: SagaStep, status: SagaStepStatus = SagaStepStatus.Created, already_rollback: bool = False,
    ):

        self.definition = definition
        self.status = status
        self.already_rollback = already_rollback

    @classmethod
    def from_raw(cls, raw: Union[dict[str, Any], SagaStepExecution], **kwargs) -> SagaStepExecution:
        """Build a new instance from a raw representation.

        :param raw: The raw representation of the instance.
        :param kwargs: Additional named arguments.
        :return: A ``SagaStepExecution`` instance.
        """
        if isinstance(raw, cls):
            return raw

        current = raw | kwargs
        current["definition"] = SagaStep.from_raw(current["definition"])
        current["status"] = SagaStepStatus.from_raw(current["status"])
        return cls(**current)

    async def execute(self, context: SagaContext, reply: Optional[CommandReply] = None, *args, **kwargs) -> SagaContext:
        """Execution the step.

        :param context: The execution context to be used during the execution.
        :param reply: An optional command reply instance (to be consumed by the on_success method).
        :return: The updated context.
        """

        await self._execute_on_execute(context, *args, **kwargs)
        context = await self._execute_on_success(context, reply, *args, **kwargs)

        self.status = SagaStepStatus.Finished
        return context

    async def _execute_on_execute(self, context: SagaContext, *args, **kwargs) -> None:
        if self.status != SagaStepStatus.Created:
            return

        self.status = SagaStepStatus.RunningOnExecute
        executor = RequestExecutor(*args, **kwargs)
        try:
            await executor.exec(self.definition.on_execute_operation, context)
        except MinosSagaFailedExecutionStepException as exc:
            self.status = SagaStepStatus.ErroredOnExecute
            raise exc
        self.status = SagaStepStatus.FinishedOnExecute

    async def _execute_on_success(
        self, context: SagaContext, reply: Optional[CommandReply] = None, *args, **kwargs
    ) -> SagaContext:
        self.status = SagaStepStatus.RunningOnSuccess
        executor = ResponseExecutor(*args, **kwargs)

        try:
            context = await executor.exec(self.definition.on_success_operation, context, reply)
        except MinosSagaPausedExecutionStepException as exc:
            self.status = SagaStepStatus.PausedOnSuccess
            raise exc
        except MinosCommandReplyFailedException as exc:
            self.status = SagaStepStatus.ErroredOnSuccess
            raise MinosSagaFailedExecutionStepException(exc)
        except MinosSagaFailedExecutionStepException as exc:
            self.status = SagaStepStatus.ErroredOnSuccess
            await self.rollback(context, *args, **kwargs)
            raise exc

        return context

    async def rollback(self, context: SagaContext, *args, **kwargs) -> SagaContext:
        """Revert the executed operation with a compensatory operation.

        :param context: Execution context.
        :return: The updated execution context.
        """
        if self.status == SagaStepStatus.Created:
            raise MinosSagaRollbackExecutionStepException("There is nothing to rollback.")

        if self.already_rollback:
            raise MinosSagaRollbackExecutionStepException("The step was already rollbacked.")

        executor = RequestExecutor(*args, **kwargs)
        await executor.exec(self.definition.on_failure_operation, context)

        self.already_rollback = True
        return context

    @property
    def raw(self) -> dict[str, Any]:
        """Compute a raw representation of the instance.

        :return: A ``dict`` instance.
        """
        return {
            "definition": self.definition.raw,
            "status": self.status.raw,
            "already_rollback": self.already_rollback,
        }

    def __eq__(self, other: SagaStep) -> bool:
        return type(self) == type(other) and tuple(self) == tuple(other)

    def __iter__(self) -> Iterable:
        yield from (
            self.definition,
            self.status,
            self.already_rollback,
        )

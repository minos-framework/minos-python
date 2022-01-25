from __future__ import (
    annotations,
)

from contextlib import (
    suppress,
)
from itertools import (
    chain,
)
from typing import (
    TYPE_CHECKING,
    Any,
    Iterable,
    Optional,
)
from uuid import (
    UUID,
)

from ...context import (
    SagaContext,
)
from ...definitions import (
    ConditionalSagaStep,
)
from ...exceptions import (
    SagaExecutionAlreadyExecutedException,
    SagaFailedExecutionStepException,
    SagaPausedExecutionStepException,
    SagaRollbackExecutionStepException,
)
from ...utils import (
    get_service_name,
)
from ..executors import (
    Executor,
)
from ..status import (
    SagaStepStatus,
)
from .abc import (
    SagaStepExecution,
)

if TYPE_CHECKING:
    from ..saga import (
        SagaExecution,
    )


class ConditionalSagaStepExecution(SagaStepExecution):
    """Conditional Saga Step Execution class."""

    inner: Optional[SagaExecution]
    definition: ConditionalSagaStep

    def __init__(self, *args, inner: Optional[SagaExecution] = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.inner = inner

    @classmethod
    def _from_raw(cls, raw: dict[str, Any]) -> SagaStepExecution:
        from ...executions import (
            SagaExecution,
        )

        raw["inner"] = SagaExecution.from_raw(raw["inner"])

        return super()._from_raw(raw)

    async def execute(self, context: SagaContext, *args, **kwargs) -> SagaContext:
        """Execution the step.

        :param context: The execution context to be used during the execution.
        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: The updated context.
        """

        if self.status == SagaStepStatus.Created:
            self.inner = await self._create_inner(context, *args, **kwargs)

        self.status = SagaStepStatus.RunningOnExecute

        if self.inner is not None:
            context = await self._execute_inner(context, *args, **kwargs)

        self.status = SagaStepStatus.Finished
        return context

    async def _create_inner(
        self, context: SagaContext, user: Optional[UUID] = None, execution_uuid: Optional[UUID] = None, *args, **kwargs
    ) -> Optional[SagaExecution]:
        from ...executions import (
            SagaExecution,
        )

        executor = Executor(execution_uuid=execution_uuid)
        self.related_services.add(get_service_name())

        definition = None
        for alternative in self.definition.if_then_alternatives:
            if await executor.exec(alternative.condition, context):
                definition = alternative.saga
                break

        if definition is None and self.definition.else_then_alternative is not None:
            definition = self.definition.else_then_alternative.saga

        if definition is None:
            return None

        return SagaExecution.from_definition(
            definition, context=context, user=user, uuid=execution_uuid, *args, **kwargs
        )

    async def _execute_inner(
        self, context: SagaContext, user: Optional[UUID] = None, execution_uuid: Optional[UUID] = None, *args, **kwargs
    ) -> SagaContext:
        execution = self.inner
        if execution_uuid is not None:
            execution.uuid = execution_uuid
        if user is not None:
            execution.user = user
        execution.context = context

        try:
            with suppress(SagaExecutionAlreadyExecutedException):
                await self.inner.execute(*args, **(kwargs | {"autocommit": False}))
        except SagaPausedExecutionStepException as exc:
            self.status = SagaStepStatus.PausedByOnExecute
            raise exc
        except SagaFailedExecutionStepException as exc:
            self.status = SagaStepStatus.ErroredByOnExecute
            raise exc

        return execution.context

    async def rollback(self, context: SagaContext, *args, **kwargs) -> SagaContext:
        """Revert the executed step.

        :param context: Execution context.
        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: The updated execution context.
        """
        if self.status == SagaStepStatus.Created:
            raise SagaRollbackExecutionStepException("There is nothing to rollback.")

        if self.already_rollback:
            raise SagaRollbackExecutionStepException("The step was already rollbacked.")

        if self.inner is not None:
            context = await self._rollback_inner(context, *args, **kwargs)

        self.already_rollback = True
        return context

    async def _rollback_inner(
        self, context: SagaContext, user: Optional[UUID] = None, execution_uuid: Optional[UUID] = None, *args, **kwargs
    ) -> SagaContext:
        execution = self.inner
        if execution_uuid is not None:
            execution.uuid = execution_uuid
        if user is not None:
            execution.user = user
        execution.context = context

        await execution.rollback(*args, **(kwargs | {"autoreject": False}))

        return execution.context

    @property
    def raw(self) -> dict[str, Any]:
        """Compute a raw representation of the instance.

        :return: A ``dict`` instance.
        """
        return super().raw | {"inner": None if self.inner is None else self.inner.raw}

    def __iter__(self) -> Iterable:
        yield from chain(super().__iter__(), (self.inner,))

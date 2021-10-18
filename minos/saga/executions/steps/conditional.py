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
    SagaFailedCommitCallbackException,
    SagaFailedExecutionStepException,
    SagaPausedExecutionStepException,
    SagaRollbackExecutionStepException,
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
    """TODO"""

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
        """TODO"""

        if self.status == SagaStepStatus.Created:
            self.inner = await self._create_inner(context, *args, **kwargs)

        self.status = SagaStepStatus.RunningOnExecute

        if self.inner is not None:
            context = await self._execute_inner(context, *args, **kwargs)

        self.status = SagaStepStatus.Finished
        return context

    async def _create_inner(
        self, context: SagaContext, execution_uuid: Optional[UUID] = None, *args, **kwargs
    ) -> Optional[SagaExecution]:
        from ...executions import (
            SagaExecution,
        )

        executor = Executor()

        for alternative in self.definition.if_then_alternatives:
            if await executor.exec(alternative.condition, context):
                return SagaExecution.from_definition(
                    alternative.saga, context=context, uuid=execution_uuid, *args, **kwargs
                )

        if self.definition.else_then_alternative is not None:
            return SagaExecution.from_definition(
                self.definition.else_then_alternative.saga, context=context, *args, **kwargs
            )

        return None

    async def _execute_inner(self, context: SagaContext, execution_uuid: UUID, *args, **kwargs) -> SagaContext:
        execution = self.inner
        if execution_uuid is not None:
            execution.uuid = execution_uuid
        try:
            execution.context = context
            with suppress(SagaExecutionAlreadyExecutedException):
                await self.inner.execute(*args, **kwargs)
        except SagaPausedExecutionStepException as exc:
            self.status = SagaStepStatus.PausedByOnExecute
            raise exc
        except SagaFailedExecutionStepException as exc:
            self.status = SagaStepStatus.ErroredByOnExecute
            raise exc
        except SagaFailedCommitCallbackException as exc:
            self.status = SagaStepStatus.ErroredByOnExecute
            raise SagaFailedExecutionStepException(exc.exception)
        return execution.context

    async def rollback(self, context: SagaContext, *args, **kwargs) -> SagaContext:
        """TODO"""

        if self.status == SagaStepStatus.Created:
            raise SagaRollbackExecutionStepException("There is nothing to rollback.")

        if self.already_rollback:
            raise SagaRollbackExecutionStepException("The step was already rollbacked.")

        if self.inner is not None:
            context = await self._rollback_inner(context, *args, **kwargs)

        self.already_rollback = True
        return context

    async def _rollback_inner(
        self, context: SagaContext, execution_uuid: Optional[UUID] = None, *args, **kwargs
    ) -> SagaContext:
        execution = self.inner
        if execution_uuid is not None:
            execution.uuid = execution_uuid
        execution.context = context

        await execution.rollback(*args, **kwargs)
        return execution.context

    @property
    def raw(self) -> dict[str, Any]:
        """Compute a raw representation of the instance.

        :return: A ``dict`` instance.
        """
        return super().raw | {"inner": None if self.inner is None else self.inner.raw}

    def __iter__(self) -> Iterable:
        yield from chain(super().__iter__(), (self.inner,))

from __future__ import (
    annotations,
)

from typing import (
    TYPE_CHECKING,
    Any,
    Optional,
    Union,
)

from ... import (
    SagaFailedCommitCallbackException,
    SagaFailedExecutionStepException,
    SagaPausedExecutionStepException,
    SagaRollbackExecutionStepException,
)
from ...context import (
    SagaContext,
)
from ...definitions import (
    ConditionalSagaStep,
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
    def from_raw(cls, raw: Union[dict[str, Any], SagaStepExecution], **kwargs) -> SagaStepExecution:
        """TODO"""
        from ...executions import (
            SagaExecution,
        )

        if "inner" in raw:
            raw["inner"] = SagaExecution.from_raw(raw["inner"])
        if "inner" in kwargs:
            kwargs["inner"] = SagaExecution.from_raw(kwargs["inner"])

        return super().from_raw(raw, **kwargs)

    async def execute(self, context: SagaContext, *args, **kwargs) -> SagaContext:
        """TODO"""

        if self.status == SagaStepStatus.Created:
            self.inner = await self._create_inner(context, *args, **kwargs)

        self.status = SagaStepStatus.RunningOnExecute

        if self.inner is not None:
            context = await self._execute_inner(*args, **kwargs)

        self.status = SagaStepStatus.Finished
        return context

    async def _create_inner(self, context: SagaContext, *args, **kwargs) -> Optional[SagaExecution]:
        from ...executions import (
            SagaExecution,
        )

        executor = Executor()

        for alternative in self.definition.if_then_alternatives:
            if await executor.exec(alternative.condition, context):
                return SagaExecution.from_definition(alternative.saga, context=context, *args, **kwargs)

        if self.definition.else_then_alternative is not None:
            return SagaExecution.from_definition(
                self.definition.else_then_alternative.saga, context=context, *args, **kwargs
            )

        return None

    async def _execute_inner(self, *args, execution_uuid=None, **kwargs) -> SagaContext:
        execution = self.inner
        try:
            await execution.execute(*args, **kwargs)
        except SagaPausedExecutionStepException as exc:
            self.status = SagaStepStatus.PausedByOnExecute
            raise exc
        except SagaFailedExecutionStepException as exc:
            self.status = SagaStepStatus.ErroredByOnExecute
            raise exc
        except SagaFailedCommitCallbackException as exc:
            self.status = SagaStepStatus.ErroredByOnExecute
            raise exc.exception
        return execution.context

    async def rollback(self, context: SagaContext, *args, execution_uuid=None, **kwargs) -> SagaContext:
        """TODO"""
        if self.status == SagaStepStatus.Created:
            raise SagaRollbackExecutionStepException("There is nothing to rollback.")

        if self.already_rollback:
            raise SagaRollbackExecutionStepException("The step was already rollbacked.")

        if self.inner is not None:
            self.inner.context = context
            await self.inner.rollback(*args, **kwargs)
            context = self.inner.context

        self.already_rollback = True
        return context

    @property
    def raw(self) -> dict[str, Any]:
        """Compute a raw representation of the instance.

        :return: A ``dict`` instance.
        """
        return super().raw | {"inner": None if self.inner is None else self.inner.raw}

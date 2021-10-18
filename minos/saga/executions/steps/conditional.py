from ...context import (
    SagaContext,
)
from .abc import (
    SagaStepExecution,
)


class ConditionalSagaStepExecution(SagaStepExecution):
    """TODO"""

    async def execute(self, context: SagaContext, *args, **kwargs) -> SagaContext:
        """TODO"""
        pass

    async def rollback(self, context: SagaContext, *args, **kwargs) -> SagaContext:
        """TODO"""
        pass

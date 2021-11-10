from typing import (
    Optional,
)

from dependency_injector.wiring import (
    Provide,
    inject,
)

from minos.common import (
    MinosConfig,
    NotProvidedException,
)

from ...context import (
    SagaContext,
)
from ...definitions import (
    LocalCallback,
    SagaOperation,
)
from ...exceptions import (
    ExecutorException,
    SagaFailedExecutionStepException,
)
from .abc import (
    Executor,
)


class LocalExecutor(Executor):
    """Local Executor class."""

    @inject
    def __init__(
        self, *args, config: MinosConfig = Provide["config"], **kwargs,
    ):
        super().__init__(*args, **kwargs)

        if config is None or isinstance(config, Provide):
            raise NotProvidedException("A config instance is required.")

        self.config = config

    @property
    def service_name(self) -> str:
        return self.config.service.name

    # noinspection PyUnusedLocal,PyMethodOverriding
    async def exec(
        self, operation: Optional[SagaOperation[LocalCallback]], context: SagaContext, *args, **kwargs
    ) -> SagaContext:
        """Execute the commit operation.

        :param operation: Operation to be executed.
        :param context: Actual execution context.
        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: An updated context instance.
        """

        if operation is None:
            return context

        try:
            context = SagaContext(**context)  # Needed to avoid mutability issues.
            new_context = await super().exec(operation, context)
        except ExecutorException as exc:
            raise SagaFailedExecutionStepException(exc.exception)

        if new_context is not None:
            context = new_context

        return context

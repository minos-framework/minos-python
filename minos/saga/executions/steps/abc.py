from __future__ import (
    annotations,
)

from abc import (
    ABC,
    abstractmethod,
)
from typing import (
    Any,
    Iterable,
    Optional,
    Union,
)

from minos.common import (
    classname,
    import_module,
)

from ...context import (
    SagaContext,
)
from ...definitions import (
    ConditionalSagaStep,
    LocalSagaStep,
    RemoteSagaStep,
    SagaStep,
)
from ..status import (
    SagaStepStatus,
)


class SagaStepExecution(ABC):
    """Saga Step Execution class."""

    def __init__(
        self,
        definition: SagaStep,
        status: SagaStepStatus = SagaStepStatus.Created,
        service_name: Optional[str] = None,
        already_rollback: bool = False,
    ):
        self.definition = definition
        self.status = status
        self.already_rollback = already_rollback

        self.service_name = service_name

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

        if "cls" in current:
            # noinspection PyTypeChecker
            execution_cls: type = import_module(current.pop("cls"))
        else:
            execution_cls = cls

        if not issubclass(execution_cls, cls):
            raise TypeError(f"Given class is not a subclass of {cls}. Obtained: {execution_cls}")

        return execution_cls._from_raw(current)

    @classmethod
    def _from_raw(cls, raw: dict[str, Any]) -> SagaStepExecution:
        raw["definition"] = SagaStep.from_raw(raw["definition"])
        raw["status"] = SagaStepStatus.from_raw(raw["status"])
        return cls(**raw)

    @staticmethod
    def from_definition(step: SagaStep) -> SagaStepExecution:
        """Build a ``SagaStepExecution`` instance from the ``SagaStep`` definition.

        :param step: The ``SagaStep`` definition.
        :return: A new ``SagaStepExecution``.
        """
        from .conditional import (
            ConditionalSagaStepExecution,
        )
        from .local import (
            LocalSagaStepExecution,
        )
        from .remote import (
            RemoteSagaStepExecution,
        )

        if isinstance(step, ConditionalSagaStep):
            return ConditionalSagaStepExecution(step)

        if isinstance(step, LocalSagaStep):
            return LocalSagaStepExecution(step)

        if isinstance(step, RemoteSagaStep):
            return RemoteSagaStepExecution(step)

        raise TypeError(f"Given step is not supported yet. Obtained: {step}")

    @abstractmethod
    async def execute(self, context: SagaContext, *args, **kwargs) -> SagaContext:
        """Execution the step.

        :param context: The execution context to be used during the execution.
        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: The updated context.
        """

    @abstractmethod
    async def rollback(self, context: SagaContext, *args, **kwargs) -> SagaContext:
        """Revert the executed step.

        :param context: Execution context.
        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: The updated execution context.
        """

    @property
    def raw(self) -> dict[str, Any]:
        """Compute a raw representation of the instance.

        :return: A ``dict`` instance.
        """
        return {
            "cls": classname(type(self)),
            "definition": self.definition.raw,
            "status": self.status.raw,
            "service_name": self.service_name,
            "already_rollback": self.already_rollback,
        }

    def __eq__(self, other: SagaStep) -> bool:
        return type(self) == type(other) and tuple(self) == tuple(other)

    def __iter__(self) -> Iterable:
        yield from (
            self.definition,
            self.status,
            self.service_name,
            self.already_rollback,
        )

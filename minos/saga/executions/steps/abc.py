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
    SagaStep,
)
from ..status import (
    SagaStepStatus,
)


class SagaStepExecution(ABC):
    """TODO"""

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

        # noinspection PyTypeChecker
        cls_: type = import_module(current.pop("cls"))
        if not issubclass(cls_, cls):
            raise TypeError("TODO")

        current["definition"] = SagaStep.from_raw(current["definition"])
        current["status"] = SagaStepStatus.from_raw(current["status"])
        return cls_(**current)

    @staticmethod
    def from_step(step: SagaStep) -> SagaStepExecution:
        """TODO"""

        from ...definitions import (
            RemoteSagaStep,
        )
        from .remote import (
            RemoteSagaStepExecution,
        )

        if isinstance(step, RemoteSagaStep):
            return RemoteSagaStepExecution(step)

        raise TypeError("TODO")

    @abstractmethod
    async def execute(self, context: SagaContext, *args, **kwargs) -> SagaContext:
        """TODO"""

    @abstractmethod
    async def rollback(self, context: SagaContext, *args, **kwargs) -> SagaContext:
        """TODO"""

    @property
    def raw(self) -> dict[str, Any]:
        """Compute a raw representation of the instance.

        :return: A ``dict`` instance.
        """
        return {
            "cls": classname(type(self)),
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

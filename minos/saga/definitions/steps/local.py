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
    classname,
)

from ...context import (
    SagaContext,
)
from ...exceptions import (
    EmptySagaStepException,
    MultipleOnExecuteException,
    MultipleOnFailureException,
    UndefinedOnExecuteException,
)
from ..operations import (
    SagaOperation,
)
from ..types import (
    LocalCallback,
)
from .abc import (
    SagaStep,
)


class LocalSagaStep(SagaStep):
    """Local Saga Step class."""

    def __init__(
        self,
        on_execute: Optional[Union[LocalCallback, SagaOperation[LocalCallback]]] = None,
        on_failure: Optional[Union[LocalCallback, SagaOperation[LocalCallback]]] = None,
        **kwargs,
    ):

        if on_execute is not None and not isinstance(on_execute, SagaOperation):
            on_execute = SagaOperation(on_execute)
        if on_failure is not None and not isinstance(on_failure, SagaOperation):
            on_failure = SagaOperation(on_failure)

        self.on_execute_operation = on_execute
        self.on_failure_operation = on_failure

        super().__init__(**kwargs)

    @classmethod
    def _from_raw(cls, raw: dict[str, Any]) -> LocalSagaStep:
        raw["on_execute"] = SagaOperation.from_raw(raw["on_execute"])
        raw["on_failure"] = SagaOperation.from_raw(raw["on_failure"])

        return cls(**raw)

    def on_execute(self, callback: LocalCallback, parameters: Optional[SagaContext] = None, **kwargs) -> LocalSagaStep:
        """On execute method.

        :param callback: The callback function to be called.
        :param parameters: A mapping of named parameters to be passed to the callback.
        :param kwargs: A set of named arguments to be passed to the callback. ``parameters`` has priority if it is not
            ``None``.
        :return: A ``self`` reference.
        """
        if self.on_execute_operation is not None:
            raise MultipleOnExecuteException()

        self.on_execute_operation = SagaOperation(callback, parameters, **kwargs)

        return self

    def on_failure(self, callback: LocalCallback, parameters: Optional[SagaContext] = None, **kwargs) -> LocalSagaStep:
        """On failure method.

        :param callback: The callback function to be called.
        :param parameters: A mapping of named parameters to be passed to the callback.
        :param kwargs: A set of named arguments to be passed to the callback. ``parameters`` has priority if it is not
            ``None``.
        :return: A ``self`` reference.
        """
        if self.on_failure_operation is not None:
            raise MultipleOnFailureException()

        self.on_failure_operation = SagaOperation(callback, parameters, **kwargs)

        return self

    def validate(self) -> None:
        """Check if the step is valid.

        :return: This method does not return anything, but raises an exception if the step is not valid.
        """
        if self.on_execute_operation is None and self.on_failure_operation is None:
            raise EmptySagaStepException()

        if self.on_execute_operation is None:
            raise UndefinedOnExecuteException()

    @property
    def raw(self) -> dict[str, Any]:
        """Generate a raw representation of the instance.

        :return: A ``dict`` instance.
        """
        return {
            "cls": classname(type(self)),
            "on_execute": None if self.on_execute_operation is None else self.on_execute_operation.raw,
            "on_failure": None if self.on_failure_operation is None else self.on_failure_operation.raw,
        }

    def __iter__(self) -> Iterable:
        yield from (
            self.on_execute_operation,
            self.on_failure_operation,
        )

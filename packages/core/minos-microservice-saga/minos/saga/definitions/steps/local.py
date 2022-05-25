"""Local Step Definitions module."""

from __future__ import (
    annotations,
)

from collections.abc import (
    Iterable,
)
from functools import (
    partial,
)
from typing import (
    Any,
    Optional,
    Protocol,
    Union,
    runtime_checkable,
)

from cached_property import (
    cached_property,
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
    SagaOperationDecorator,
)
from ..types import (
    LocalCallback,
)
from .abc import (
    SagaStep,
    SagaStepDecoratorMeta,
    SagaStepDecoratorWrapper,
)


@runtime_checkable
class LocalSagaStepDecoratorWrapper(SagaStepDecoratorWrapper, Protocol):
    """Local Saga Step Decorator Wrapper class."""

    meta: LocalSagaStepDecoratorMeta
    on_failure: type[SagaOperationDecorator[LocalCallback]]
    __call__: LocalCallback


class LocalSagaStepDecoratorMeta(SagaStepDecoratorMeta):
    """Local Saga Step Decorator Meta class."""

    _definition: LocalSagaStep
    _on_failure: Optional[LocalCallback]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._on_failure = None

    @cached_property
    def definition(self):
        """Get the step definition.

        :return: A ``SagaStep`` instance.
        """
        self._definition.on_execute(self._inner)
        if self._on_failure is not None:
            self._definition.on_failure(self._on_failure)
        return self._definition

    @cached_property
    def on_failure(self) -> SagaOperationDecorator:
        """Get the on failure decorator.

        :return: A ``SagaOperationDecorator`` type.
        """
        # noinspection PyTypeChecker
        return partial(SagaOperationDecorator[LocalCallback], step_meta=self, attr_name="_on_failure")


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

    def __call__(self, func: LocalCallback) -> LocalSagaStepDecoratorWrapper:
        """Decorate the given function.

        :param func: The function to be decorated.
        :return: The decorated function.
        """
        meta = LocalSagaStepDecoratorMeta(func, self)
        func.meta = meta
        func.on_failure = meta.on_failure
        return func

    def on_execute(
        self, operation: Union[SagaOperation, LocalCallback], parameters: Optional[SagaContext] = None, **kwargs
    ) -> LocalSagaStep:
        """On execute method.

        :param operation: The callback function to be called.
        :param parameters: A mapping of named parameters to be passed to the callback.
        :param kwargs: A set of named arguments to be passed to the callback. ``parameters`` has priority if it is not
            ``None``.
        :return: A ``self`` reference.
        """
        if self.on_execute_operation is not None:
            raise MultipleOnExecuteException()

        if not isinstance(operation, SagaOperation):
            operation = SagaOperation(operation, parameters, **kwargs)

        self.on_execute_operation = operation

        return self

    def on_failure(
        self, operation: Union[SagaOperation, LocalCallback], parameters: Optional[SagaContext] = None, **kwargs
    ) -> LocalSagaStep:
        """On failure method.

        :param operation: The callback function to be called.
        :param parameters: A mapping of named parameters to be passed to the callback.
        :param kwargs: A set of named arguments to be passed to the callback. ``parameters`` has priority if it is not
            ``None``.
        :return: A ``self`` reference.
        """
        if self.on_failure_operation is not None:
            raise MultipleOnFailureException()

        if not isinstance(operation, SagaOperation):
            operation = SagaOperation(operation, parameters, **kwargs)

        self.on_failure_operation = operation

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
            "order": self.order,
            "on_execute": None if self.on_execute_operation is None else self.on_execute_operation.raw,
            "on_failure": None if self.on_failure_operation is None else self.on_failure_operation.raw,
        }

    def __iter__(self) -> Iterable:
        yield from (
            self.order,
            self.on_execute_operation,
            self.on_failure_operation,
        )

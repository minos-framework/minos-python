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
    Union,
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
)
from ..types import (
    LocalCallback,
)
from .abc import (
    OnSagaStepDecorator,
    SagaStep,
    SagaStepDecoratorMeta,
    SagaStepDecoratorWrapper,
)


class LocalSagaStepDecoratorWrapper(SagaStepDecoratorWrapper):
    """TODO"""

    meta: LocalSagaStepDecoratorMeta
    on_failure: type[OnSagaStepDecorator[LocalCallback]]
    __call__: LocalCallback


class LocalSagaStepDecoratorMeta(SagaStepDecoratorMeta):
    """TODO"""

    _definition: LocalSagaStep
    _on_failure: Optional[LocalCallback]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._on_failure = None

    @cached_property
    def definition(self):
        """TODO"""
        self._definition.on_execute(self._inner)
        if self._on_failure is not None:
            self._definition.on_failure(self._on_failure)
        return self._definition

    @cached_property
    def on_failure(self) -> OnSagaStepDecorator:
        """TODO"""
        # noinspection PyTypeChecker
        return partial(OnSagaStepDecorator, step_meta=self, attr_name="_on_failure")


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
        meta = LocalSagaStepDecoratorMeta(func, self)
        func.meta = meta
        func.on_failure = meta.on_failure
        # noinspection PyTypeChecker
        return func

    def on_execute(self, callback: LocalCallback, parameters: Optional[SagaContext] = None, **kwargs) -> LocalSagaStep:
        """On execute method.

        :param callback: The callback function to be called.
        :param parameters: A mapping of named parameters to be passed to the callback.
        :param kwargs: A set of named arguments to be passed to the callback. ``parameters`` has order if it is not
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
        :param kwargs: A set of named arguments to be passed to the callback. ``parameters`` has order if it is not
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

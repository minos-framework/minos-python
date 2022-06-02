"""Remote Step Definitions module."""

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
    MultipleOnErrorException,
    MultipleOnExecuteException,
    MultipleOnFailureException,
    MultipleOnSuccessException,
    UndefinedOnExecuteException,
)
from ..operations import (
    SagaOperation,
    SagaOperationDecorator,
)
from ..types import (
    RequestCallBack,
    ResponseCallBack,
)
from .abc import (
    SagaStep,
    SagaStepDecoratorMeta,
    SagaStepDecoratorWrapper,
)


@runtime_checkable
class RemoteSagaStepDecoratorWrapper(SagaStepDecoratorWrapper, Protocol):
    """Remote Saga Step Decorator Wrapper class."""

    meta: RemoteSagaStepDecoratorMeta
    on_success: type[SagaOperationDecorator[ResponseCallBack]]
    on_error: type[SagaOperationDecorator[ResponseCallBack]]
    on_failure: type[SagaOperationDecorator[RequestCallBack]]
    __call__: RequestCallBack


class RemoteSagaStepDecoratorMeta(SagaStepDecoratorMeta):
    """Remote Saga Step Decorator Meta class."""

    _definition: RemoteSagaStep
    _on_success: Optional[ResponseCallBack]
    _on_error: Optional[ResponseCallBack]
    _on_failure: Optional[RequestCallBack]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._on_success = None
        self._on_error = None
        self._on_failure = None

    @cached_property
    def definition(self):
        """Get the step definition.

        :return: A ``SagaStep`` instance.
        """
        self._definition.on_execute(self._inner)
        if self._on_success is not None:
            self._definition.on_success(self._on_success)
        if self._on_error is not None:
            self._definition.on_error(self._on_error)
        if self._on_failure is not None:
            self._definition.on_failure(self._on_failure)
        return self._definition

    @cached_property
    def on_success(self) -> SagaOperationDecorator:
        """Get the on success decorator.

        :return: A ``SagaOperationDecorator`` type.
        """
        # noinspection PyTypeChecker
        return partial(SagaOperationDecorator[ResponseCallBack], step_meta=self, attr_name="_on_success")

    @cached_property
    def on_error(self) -> SagaOperationDecorator:
        """Get the on error decorator.

        :return: A ``SagaOperationDecorator`` type.
        """
        # noinspection PyTypeChecker
        return partial(SagaOperationDecorator[ResponseCallBack], step_meta=self, attr_name="_on_error")

    @cached_property
    def on_failure(self) -> SagaOperationDecorator:
        """Get the on failure decorator.

        :return: A ``SagaOperationDecorator`` type.
        """
        # noinspection PyTypeChecker
        return partial(SagaOperationDecorator[RequestCallBack], step_meta=self, attr_name="_on_failure")


class RemoteSagaStep(SagaStep):
    """Remote Saga Step class."""

    def __init__(
        self,
        on_execute: Optional[Union[RequestCallBack, SagaOperation[RequestCallBack]]] = None,
        on_success: Optional[Union[ResponseCallBack, SagaOperation[ResponseCallBack]]] = None,
        on_error: Optional[Union[ResponseCallBack, SagaOperation[ResponseCallBack]]] = None,
        on_failure: Optional[Union[RequestCallBack, SagaOperation[RequestCallBack]]] = None,
        **kwargs,
    ):

        if on_execute is not None and not isinstance(on_execute, SagaOperation):
            on_execute = SagaOperation(on_execute)
        if on_failure is not None and not isinstance(on_failure, SagaOperation):
            on_failure = SagaOperation(on_failure)
        if on_success is not None and not isinstance(on_success, SagaOperation):
            on_success = SagaOperation(on_success)
        if on_error is not None and not isinstance(on_error, SagaOperation):
            on_error = SagaOperation(on_error)

        self.on_execute_operation = on_execute
        self.on_failure_operation = on_failure
        self.on_success_operation = on_success
        self.on_error_operation = on_error

        super().__init__(**kwargs)

    @classmethod
    def _from_raw(cls, raw: dict[str, Any]) -> RemoteSagaStep:
        raw["on_execute"] = SagaOperation.from_raw(raw["on_execute"])
        raw["on_failure"] = SagaOperation.from_raw(raw["on_failure"])
        raw["on_success"] = SagaOperation.from_raw(raw["on_success"])
        raw["on_error"] = SagaOperation.from_raw(raw["on_error"])

        return cls(**raw)

    def __call__(self, func: RequestCallBack) -> RemoteSagaStepDecoratorWrapper:
        """Decorate the given function.

        :param func: The function to be decorated.
        :return: The decorated function.
        """
        meta = RemoteSagaStepDecoratorMeta(func, self)
        func.meta = meta
        func.on_success = meta.on_success
        func.on_error = meta.on_error
        func.on_failure = meta.on_failure
        return func

    def on_execute(
        self,
        operation: Union[SagaOperation[RequestCallBack], RequestCallBack],
        parameters: Optional[SagaContext] = None,
        **kwargs,
    ) -> RemoteSagaStep:
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
        self,
        operation: Union[SagaOperation[RequestCallBack], RequestCallBack],
        parameters: Optional[SagaContext] = None,
        **kwargs,
    ) -> RemoteSagaStep:
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

    def on_success(
        self,
        operation: Union[SagaOperation[ResponseCallBack], ResponseCallBack],
        parameters: Optional[SagaContext] = None,
        **kwargs,
    ) -> RemoteSagaStep:
        """On success method.

        :param operation: The callback function to be called.
        :param parameters: A mapping of named parameters to be passed to the callback.
        :param kwargs: A set of named arguments to be passed to the callback. ``parameters`` has priority if it is not
            ``None``.
        :return: A ``self`` reference.
        """
        if self.on_success_operation is not None:
            raise MultipleOnSuccessException()

        if not isinstance(operation, SagaOperation):
            operation = SagaOperation(operation, parameters, **kwargs)

        self.on_success_operation = operation

        return self

    def on_error(
        self,
        operation: Union[SagaOperation[ResponseCallBack], ResponseCallBack],
        parameters: Optional[SagaContext] = None,
        **kwargs,
    ) -> RemoteSagaStep:
        """On error method.

        :param operation: The callback function to be called.
        :param parameters: A mapping of named parameters to be passed to the callback.
        :param kwargs: A set of named arguments to be passed to the callback. ``parameters`` has priority if it is not
            ``None``.
        :return: A ``self`` reference.
        """
        if self.on_error_operation is not None:
            raise MultipleOnErrorException()

        if not isinstance(operation, SagaOperation):
            operation = SagaOperation(operation, parameters, **kwargs)

        self.on_error_operation = operation

        return self

    def validate(self) -> None:
        """Check if the step is valid.

        :return: This method does not return anything, but raises an exception if the step is not valid.
        """
        if (
            self.on_execute_operation is None
            and self.on_failure_operation is None
            and self.on_success_operation is None
            and self.on_error_operation is None
        ):
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
            "on_success": None if self.on_success_operation is None else self.on_success_operation.raw,
            "on_error": None if self.on_error_operation is None else self.on_error_operation.raw,
        }

    def __iter__(self) -> Iterable:
        yield from (
            self.order,
            self.on_execute_operation,
            self.on_failure_operation,
            self.on_success_operation,
            self.on_error_operation,
        )

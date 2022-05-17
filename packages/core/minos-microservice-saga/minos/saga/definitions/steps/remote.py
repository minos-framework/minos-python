from __future__ import (
    annotations,
)

from collections.abc import (
    Callable,
)
from typing import (
    Any,
    Iterable,
    Optional,
    TypeVar,
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
    MultipleOnErrorException,
    MultipleOnExecuteException,
    MultipleOnFailureException,
    MultipleOnSuccessException,
    UndefinedOnExecuteException,
)
from ..operations import (
    SagaOperation,
)
from ..types import (
    RequestCallBack,
    ResponseCallBack,
)
from .abc import (
    SagaStep,
    SagaStepMeta,
    SagaStepWrapper,
)

T = TypeVar("T")


class RemoteSagaStepWrapper(SagaStepWrapper):
    """TODO"""

    meta: RemoteSagaStepMeta
    on_success: Callable
    on_error: Callable
    on_failure: Callable


class RemoteSagaStepMeta(SagaStepMeta):
    """TODO"""

    func: T
    _saga_step: RemoteSagaStep
    _on_success: Optional[Callable]
    _on_error: Optional[Callable]
    _on_failure: Optional[Callable]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._on_success = None
        self._on_error = None
        self._on_failure = None

    @cached_property
    def saga_step(self):
        """TODO"""
        self._saga_step.on_execute(self.func)
        if self._on_success is not None:
            self._saga_step.on_success(self._on_success)
        if self._on_error is not None:
            self._saga_step.on_error(self._on_error)
        if self._on_failure is not None:
            self._saga_step.on_failure(self._on_failure)
        return self._saga_step

    def on_success(self, *args, **kwargs):
        return OnSuccessRemoteStepDecorator(*args, **kwargs, remote_step_meta=self)

    def on_error(self, *args, **kwargs):
        return OnErrorRemoteStepDecorator(*args, **kwargs, remote_step_meta=self)

    def on_failure(self, *args, **kwargs):
        return OnFailureRemoteStepDecorator(*args, **kwargs, remote_step_meta=self)


class OnSuccessRemoteStepDecorator:
    """ "TODO"""

    def __init__(self, remote_step_meta, *args, **kwargs):
        self.remote_step_meta = remote_step_meta

    def __call__(self, func):
        self.remote_step_meta._on_success = func
        return func


class OnErrorRemoteStepDecorator:
    """ "TODO"""

    def __init__(self, remote_step_meta, *args, **kwargs):
        self.remote_step_meta = remote_step_meta

    def __call__(self, func):
        self.remote_step_meta._on_error = func
        return func


class OnFailureRemoteStepDecorator:
    """ "TODO"""

    def __init__(self, remote_step_meta, *args, **kwargs):
        self.remote_step_meta = remote_step_meta

    def __call__(self, func):
        self.remote_step_meta._on_failure = func
        return func


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

    def __call__(self, func: Union[Callable, RemoteSagaStepWrapper]) -> RemoteSagaStepWrapper:
        func.meta = RemoteSagaStepMeta(func, self)
        func.on_success = func.meta.on_success
        func.on_error = func.meta.on_error
        func.on_failure = func.meta.on_failure
        return func

    def on_execute(
        self, callback: RequestCallBack, parameters: Optional[SagaContext] = None, **kwargs
    ) -> RemoteSagaStep:
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

    def on_failure(
        self, callback: RequestCallBack, parameters: Optional[SagaContext] = None, **kwargs
    ) -> RemoteSagaStep:
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

    def on_success(
        self, callback: ResponseCallBack, parameters: Optional[SagaContext] = None, **kwargs
    ) -> RemoteSagaStep:
        """On success method.

        :param callback: The callback function to be called.
        :param parameters: A mapping of named parameters to be passed to the callback.
        :param kwargs: A set of named arguments to be passed to the callback. ``parameters`` has priority if it is not
            ``None``.
        :return: A ``self`` reference.
        """
        if self.on_success_operation is not None:
            raise MultipleOnSuccessException()

        self.on_success_operation = SagaOperation(callback, parameters, **kwargs)

        return self

    def on_error(
        self, callback: ResponseCallBack, parameters: Optional[SagaContext] = None, **kwargs
    ) -> RemoteSagaStep:
        """On error method.

        :param callback: The callback function to be called.
        :param parameters: A mapping of named parameters to be passed to the callback.
        :param kwargs: A set of named arguments to be passed to the callback. ``parameters`` has priority if it is not
            ``None``.
        :return: A ``self`` reference.
        """
        if self.on_error_operation is not None:
            raise MultipleOnErrorException()

        self.on_error_operation = SagaOperation(callback, parameters, **kwargs)

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
            "on_execute": None if self.on_execute_operation is None else self.on_execute_operation.raw,
            "on_failure": None if self.on_failure_operation is None else self.on_failure_operation.raw,
            "on_success": None if self.on_success_operation is None else self.on_success_operation.raw,
            "on_error": None if self.on_error_operation is None else self.on_error_operation.raw,
        }

    def __iter__(self) -> Iterable:
        yield from (
            self.on_execute_operation,
            self.on_failure_operation,
            self.on_success_operation,
            self.on_error_operation,
        )

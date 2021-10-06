from __future__ import (
    annotations,
)

from typing import (
    TYPE_CHECKING,
    Any,
    Iterable,
    Optional,
    Union,
)

from ..context import (
    SagaContext,
)
from ..exceptions import (
    MinosMultipleOnExecuteException,
    MinosMultipleOnFailureException,
    MinosMultipleOnSuccessException,
    MinosSagaEmptyStepException,
    MinosSagaNotDefinedException,
    MinosUndefinedOnExecuteException,
)
from .operations import (
    SagaOperation,
)
from .types import (
    RequestCallBack,
    ResponseCallBack,
)

if TYPE_CHECKING:
    from .saga import (
        Saga,
    )


class SagaStep:
    """Saga step class."""

    def __init__(
        self,
        on_execute: Optional[Union[RequestCallBack, SagaOperation]] = None,
        on_success: Optional[Union[ResponseCallBack, SagaOperation]] = None,
        on_failure: Optional[Union[RequestCallBack, SagaOperation]] = None,
        saga: Optional[Saga] = None,
    ):
        if on_execute is not None and not isinstance(on_execute, SagaOperation):
            on_execute = SagaOperation(on_execute)
        if on_failure is not None and not isinstance(on_failure, SagaOperation):
            on_failure = SagaOperation(on_failure)
        if on_success is not None and not isinstance(on_success, SagaOperation):
            on_success = SagaOperation(on_success)

        self.on_execute_operation = on_execute
        self.on_failure_operation = on_failure
        self.on_success_operation = on_success

        self.saga = saga

    @classmethod
    def from_raw(cls, raw: Union[dict[str, Any], SagaStep], **kwargs) -> SagaStep:
        """Build a new instance from raw.

        :param raw: A raw representation.
        :param kwargs: Additional named arguments.
        :return: A ``SagaStep`` instance.
        """
        if isinstance(raw, cls):
            return raw

        current = raw | kwargs

        current["on_execute"] = SagaOperation.from_raw(current["on_execute"])
        current["on_failure"] = SagaOperation.from_raw(current["on_failure"])
        current["on_success"] = SagaOperation.from_raw(current["on_success"])

        return cls(**current)

    def on_execute(self, callback: RequestCallBack, parameters: Optional[SagaContext] = None, **kwargs) -> SagaStep:
        """On execute method.

        :param callback: The callback function to be called.
        :param parameters: A mapping of named parameters to be passed to the callback.
        :param kwargs: A set of named arguments to be passed to the callback. ``parameters`` has priority if it is not
            ``None``.
        :return: A ``self`` reference.
        """
        if self.on_execute_operation is not None:
            raise MinosMultipleOnExecuteException()

        self.on_execute_operation = SagaOperation(callback, parameters, **kwargs)

        return self

    def on_failure(self, callback: RequestCallBack, parameters: Optional[SagaContext] = None, **kwargs) -> SagaStep:
        """On failure method.

        :param callback: The callback function to be called.
        :param parameters: A mapping of named parameters to be passed to the callback.
        :param kwargs: A set of named arguments to be passed to the callback. ``parameters`` has priority if it is not
            ``None``.
        :return: A ``self`` reference.
        """
        if self.on_failure_operation is not None:
            raise MinosMultipleOnFailureException()

        self.on_failure_operation = SagaOperation(callback, parameters, **kwargs)

        return self

    def on_success(self, callback: ResponseCallBack, parameters: Optional[SagaContext] = None, **kwargs) -> SagaStep:
        """On success method.

        :param callback: The callback function to be called.
        :param parameters: A mapping of named parameters to be passed to the callback.
        :param kwargs: A set of named arguments to be passed to the callback. ``parameters`` has priority if it is not
            ``None``.
        :return: A ``self`` reference.
        """
        if self.on_success_operation is not None:
            raise MinosMultipleOnSuccessException()

        self.on_success_operation = SagaOperation(callback, parameters, **kwargs)

        return self

    def step(self, *args, **kwargs) -> SagaStep:
        """Create a new step in the ``Saga``.

        :param args: Additional positional parameters.
        :param kwargs: Additional named parameters.
        :return: A new ``SagaStep`` instance.
        """
        self.validate()
        if self.saga is None:
            raise MinosSagaNotDefinedException()
        return self.saga.step(*args, **kwargs)

    def commit(self, *args, **kwargs) -> Saga:
        """Commit the current ``SagaStep`` on the ``Saga``.

        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: A ``Saga`` instance.
        """
        self.validate()
        if self.saga is None:
            raise MinosSagaNotDefinedException()
        return self.saga.commit(*args, **kwargs)

    def validate(self) -> None:
        """Performs a validation about the structure of the defined ``SagaStep``.

        :return This method does not return anything.
        """
        if (
            self.on_execute_operation is None
            and self.on_failure_operation is None
            and self.on_success_operation is None
        ):
            raise MinosSagaEmptyStepException()

        if self.on_execute_operation is None:
            raise MinosUndefinedOnExecuteException()

    @property
    def raw(self) -> dict[str, Any]:
        """Generate a raw representation of the instance.

        :return: A ``dict`` instance.
        """
        return {
            "on_execute": None if self.on_execute_operation is None else self.on_execute_operation.raw,
            "on_failure": None if self.on_failure_operation is None else self.on_failure_operation.raw,
            "on_success": None if self.on_success_operation is None else self.on_success_operation.raw,
        }

    def __eq__(self, other: SagaStep) -> bool:
        return type(self) == type(other) and tuple(self) == tuple(other)

    def __iter__(self) -> Iterable:
        yield from (
            self.on_execute_operation,
            self.on_failure_operation,
            self.on_success_operation,
        )

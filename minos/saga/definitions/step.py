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
    MinosMultipleInvokeParticipantException,
    MinosMultipleOnReplyException,
    MinosMultipleWithCompensationException,
    MinosSagaEmptyStepException,
    MinosSagaNotDefinedException,
    MinosUndefinedInvokeParticipantException,
)
from .operations import (
    SagaOperation,
)
from .types import (
    PublishCallBack,
    ReplyCallBack,
)

if TYPE_CHECKING:
    from .saga import (
        Saga,
    )


class SagaStep(object):
    """Saga step class."""

    def __init__(
        self,
        saga: Optional[Saga] = None,
        invoke_participant: Optional[SagaOperation] = None,
        on_failure: Optional[SagaOperation] = None,
        on_success: Optional[SagaOperation] = None,
    ):
        self.saga = saga
        self.invoke_participant_operation = invoke_participant
        self.on_failure_operation = on_failure
        self.on_success_operation = on_success

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

        current["invoke_participant"] = SagaOperation.from_raw(current["invoke_participant"])
        current["on_failure"] = SagaOperation.from_raw(current["on_failure"])
        current["on_success"] = SagaOperation.from_raw(current["on_success"])

        return cls(**current)

    def invoke_participant(self, callback: PublishCallBack, parameters: Optional[SagaContext] = None) -> SagaStep:
        """Invoke a new participant method.

        :param callback: The callback function used for the request contents preparation.
        :param parameters: A mapping of named parameters to be passed to the callback.
        :return: A ``self`` reference.
        """
        if self.invoke_participant_operation is not None:
            raise MinosMultipleInvokeParticipantException()

        self.invoke_participant_operation = SagaOperation(callback, parameters)

        return self

    def on_failure(self, callback: PublishCallBack, parameters: Optional[SagaContext] = None) -> SagaStep:
        """With compensation method.

        :param callback: The callback function used for the request contents preparation.
        :param parameters: A mapping of named parameters to be passed to the callback.
        :return: A ``self`` reference.
        """
        if self.on_failure_operation is not None:
            raise MinosMultipleWithCompensationException()

        self.on_failure_operation = SagaOperation(callback, parameters)

        return self

    def on_success(self, callback: ReplyCallBack, parameters: Optional[SagaContext] = None) -> SagaStep:
        """On reply method.

        :param callback: The callback function used to handle the invoke participant response.
        :param parameters: A mapping of named parameters to be passed to the callback.
        :return: A ``self`` reference.
        """
        if self.on_success_operation is not None:
            raise MinosMultipleOnReplyException()

        self.on_success_operation = SagaOperation(callback, parameters)

        return self

    @property
    def has_reply(self) -> bool:
        """Check if the step contains a reply operation or not.

        :return: A ``bool`` instance.
        """
        return self.on_success_operation is not None

    def step(self) -> SagaStep:
        """Create a new step in the ``Saga``.

        :return: A new ``SagaStep`` instance.
        """
        self.validate()
        if self.saga is None:
            raise MinosSagaNotDefinedException()
        return self.saga.step()

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
            self.invoke_participant_operation is None
            and self.on_failure_operation is None
            and self.on_success_operation is None
        ):
            raise MinosSagaEmptyStepException()

        if self.invoke_participant_operation is None:
            raise MinosUndefinedInvokeParticipantException()

    @property
    def raw(self) -> dict[str, Any]:
        """Generate a raw representation of the instance.

        :return: A ``dict`` instance.
        """
        return {
            "invoke_participant": (
                None if self.invoke_participant_operation is None else self.invoke_participant_operation.raw
            ),
            "on_failure": (None if self.on_failure_operation is None else self.on_failure_operation.raw),
            "on_success": (None if self.on_success_operation is None else self.on_success_operation.raw),
        }

    def __eq__(self, other: SagaStep) -> bool:
        return type(self) == type(other) and tuple(self) == tuple(other)

    def __iter__(self) -> Iterable:
        yield from (
            self.invoke_participant_operation,
            self.on_failure_operation,
            self.on_success_operation,
        )

"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

from typing import (
    TYPE_CHECKING,
    Any,
    Iterable,
    NoReturn,
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
    identity_fn,
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
        with_compensation: Optional[SagaOperation] = None,
        on_reply: Optional[SagaOperation] = None,
    ):
        self.saga = saga
        self.invoke_participant_operation = invoke_participant
        self.with_compensation_operation = with_compensation
        self.on_reply_operation = on_reply

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
        current["with_compensation"] = SagaOperation.from_raw(current["with_compensation"])
        current["on_reply"] = SagaOperation.from_raw(current["on_reply"])

        return cls(**current)

    def invoke_participant(
        self, name: Union[str, list], callback: PublishCallBack, parameters: Optional[SagaContext] = None
    ) -> SagaStep:
        """Invoke a new participant method.

        :param name: The name of the new participant instruction.
        :param callback: The callback function used for the request contents preparation.
        :param parameters: A mapping of named parameters to be passed to the callback.
        :return: A ``self`` reference.
        """
        if self.invoke_participant_operation is not None:
            raise MinosMultipleInvokeParticipantException()

        self.invoke_participant_operation = SagaOperation(callback, name, parameters)

        return self

    def with_compensation(
        self, name: str, callback: PublishCallBack, parameters: Optional[SagaContext] = None
    ) -> SagaStep:
        """With compensation method.

        :param name: The name of the with compensation instruction.
        :param callback: The callback function used for the request contents preparation.
        :param parameters: A mapping of named parameters to be passed to the callback.
        :return: A ``self`` reference.
        """
        if self.with_compensation_operation is not None:
            raise MinosMultipleWithCompensationException()

        self.with_compensation_operation = SagaOperation(callback, name, parameters)

        return self

    def on_reply(
        self, name: str, callback: ReplyCallBack = identity_fn, parameters: Optional[SagaContext] = None
    ) -> SagaStep:
        """On reply method.

        :param name: The name of the variable in which the reply will be stored on the context.
        :param callback: The callback function used to handle the invoke participant response.
        :param parameters: A mapping of named parameters to be passed to the callback.
        :return: A ``self`` reference.
        """
        if self.on_reply_operation is not None:
            raise MinosMultipleOnReplyException()

        self.on_reply_operation = SagaOperation(callback, name, parameters)

        return self

    @property
    def has_reply(self) -> bool:
        """Check if the step contains a reply operation or not.

        :return: A ``bool`` instance.
        """
        return self.on_reply_operation is not None

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

    def validate(self) -> NoReturn:
        """Performs a validation about the structure of the defined ``SagaStep``.

        :return This method does not return anything.
        """
        if (
            self.invoke_participant_operation is None
            and self.with_compensation_operation is None
            and self.on_reply_operation is None
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
            "with_compensation": (
                None if self.with_compensation_operation is None else self.with_compensation_operation.raw
            ),
            "on_reply": (None if self.on_reply_operation is None else self.on_reply_operation.raw),
        }

    def __eq__(self, other: SagaStep) -> bool:
        return type(self) == type(other) and tuple(self) == tuple(other)

    def __iter__(self) -> Iterable:
        yield from (
            self.invoke_participant_operation,
            self.with_compensation_operation,
            self.on_reply_operation,
        )

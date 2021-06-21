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
    Callable,
    Coroutine,
    Iterable,
    NoReturn,
    Optional,
    TypeVar,
    Union,
)

from minos.common import (
    classname,
    import_module,
)

from ..exceptions import (
    MinosMultipleInvokeParticipantException,
    MinosMultipleOnReplyException,
    MinosMultipleWithCompensationException,
    MinosSagaEmptyStepException,
    MinosSagaNotDefinedException,
    MinosUndefinedInvokeParticipantException,
)

if TYPE_CHECKING:
    from minos.common import (
        Model,
    )

    from ..executions import (
        SagaContext,
    )
    from .saga import (
        Saga,
    )

    T = TypeVar("T")

    CallBack = Callable[
        [SagaContext], Union[Model, list[Model], Coroutine[Any, Any, Model], Coroutine[Any, Any, list[Model]]],
    ]


def identity_fn(x: T) -> T:
    """A identity function, that returns the same value without any transformation.

    :param x: The input value.
    :return: This function return the input value without any transformation.
    """
    return x


class SagaStepOperation(object):
    """Saga Step Operation class."""

    def __init__(self, name: str, callback: Callable):
        self.name = name
        self.callback = callback

    @property
    def raw(self) -> dict[str, Any]:
        """Generate a rew representation of the instance.

        :return: A ``dict`` instance.
        """
        # noinspection PyTypeChecker
        return {"name": self.name, "callback": classname(self.callback)}

    @classmethod
    def from_raw(cls, raw: Optional[Union[dict[str, Any], SagaStepOperation]], **kwargs) -> Optional[SagaStepOperation]:
        """Build a new instance from a raw representation.

        :param raw: A raw representation.
        :param kwargs: Additional named arguments.
        :return: A ``SagaStepOperation`` instance if the ``raw`` argument is not ``None``, ``None`` otherwise.
        """
        if raw is None:
            return None

        if isinstance(raw, cls):
            return raw

        current = raw | kwargs
        if isinstance(current["callback"], str):
            current["callback"] = import_module(current["callback"])
        return cls(**current)

    def __eq__(self, other: SagaStep) -> bool:
        return type(self) == type(other) and tuple(self) == tuple(other)

    def __iter__(self) -> Iterable:
        yield from (
            self.name,
            self.callback,
        )


class SagaStep(object):
    """Saga step class."""

    def __init__(
        self,
        saga: Optional[Saga] = None,
        invoke_participant: Optional[SagaStepOperation] = None,
        with_compensation: Optional[SagaStepOperation] = None,
        on_reply: Optional[SagaStepOperation] = None,
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

        current["invoke_participant"] = SagaStepOperation.from_raw(current["invoke_participant"])
        current["with_compensation"] = SagaStepOperation.from_raw(current["with_compensation"])
        current["on_reply"] = SagaStepOperation.from_raw(current["on_reply"])

        return cls(**current)

    def invoke_participant(self, name: Union[str, list], callback: CallBack) -> SagaStep:
        """Invoke a new participant method.

        :param name: The name of the new participant instruction.
        :param callback: The callback function used for the request contents preparation.
        :return: A ``self`` reference.
        """
        if self.invoke_participant_operation is not None:
            raise MinosMultipleInvokeParticipantException()

        self.invoke_participant_operation = SagaStepOperation(name, callback)

        return self

    def with_compensation(self, name: str, callback: CallBack) -> SagaStep:
        """With compensation method.

        :param name: The name of the with compensation instruction.
        :param callback: The callback function used for the request contents preparation.
        :return: A ``self`` reference.
        """
        if self.with_compensation_operation is not None:
            raise MinosMultipleWithCompensationException()

        self.with_compensation_operation = SagaStepOperation(name, callback)

        return self

    def on_reply(self, name: str, callback: Callable = identity_fn) -> SagaStep:
        """On reply method.

        :param name: The name of the variable in which the reply will be stored on the context.
        :param callback: The callback function used to handle the invoke participant response.
        :return: A ``self`` reference.
        """
        if self.on_reply_operation is not None:
            raise MinosMultipleOnReplyException()

        self.on_reply_operation = SagaStepOperation(name, callback)

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

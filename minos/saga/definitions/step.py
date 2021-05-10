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
        MinosModel,
    )

    from ..executions import (
        SagaContext,
    )
    from .saga import (
        Saga,
    )

    CallBack = Callable[
        [SagaContext],
        Union[MinosModel, list[MinosModel], Coroutine[Any, Any, MinosModel], Coroutine[Any, Any, list[MinosModel]]],
    ]


def identity_fn(x):
    """TODO

    :param x: TODO
    :return: TODO
    """
    return x


class SagaStepOperation(object):
    """TODO"""

    def __init__(self, name: str, callback: Callable):
        self.name = name
        self.callback = callback

    @property
    def raw(self) -> dict[str, Any]:
        """TODO

        :return: TODO
        """
        return {"name": self.name, "callback": classname(self.callback)}

    @classmethod
    def from_raw(cls, raw: dict[str, Any], **kwargs) -> Optional[SagaStepOperation]:
        """TODO

        :param raw: TODO
        :param kwargs: TODO
        :return: TODO
        """
        if raw is None:
            return None
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
    """TODO"""

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
        """TODO

        :param raw: TODO
        :param kwargs: TODO
        :return: TODO
        """
        if isinstance(raw, cls):
            return raw

        current = raw | kwargs

        current["invoke_participant"] = SagaStepOperation.from_raw(current["invoke_participant"])
        current["with_compensation"] = SagaStepOperation.from_raw(current["with_compensation"])
        current["on_reply"] = SagaStepOperation.from_raw(current["on_reply"])

        return cls(**current)

    def invoke_participant(self, name: Union[str, list], callback: CallBack) -> SagaStep:
        """TODO

        :param name: TODO
        :param callback: TODO
        :return: TODO
        """
        if self.invoke_participant_operation is not None:
            raise MinosMultipleInvokeParticipantException()

        self.invoke_participant_operation = SagaStepOperation(name, callback)

        return self

    def with_compensation(self, name: str, callback: CallBack) -> SagaStep:
        """TODO

        :param name: TODO
        :param callback: TODO
        :return: TODO
        """
        if self.with_compensation_operation is not None:
            raise MinosMultipleWithCompensationException()

        self.with_compensation_operation = SagaStepOperation(name, callback)

        return self

    def on_reply(self, name: str, callback: Callable = identity_fn) -> SagaStep:
        """TODO

        :param name: TODO
        :param callback: TODO
        :return: TODO
        """
        if self.on_reply_operation is not None:
            raise MinosMultipleOnReplyException()

        self.on_reply_operation = SagaStepOperation(name, callback)

        return self

    def step(self) -> SagaStep:
        """TODO

        :return: TODO
        """
        self.validate()
        if self.saga is None:
            raise MinosSagaNotDefinedException()
        return self.saga.step()

    def commit(self) -> Saga:
        """TODO

        :return: TODO
        """
        self.validate()
        if self.saga is None:
            raise MinosSagaNotDefinedException()
        return self.saga

    def validate(self) -> NoReturn:
        """TODO

        :return TODO:
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
        """TODO

        :return: TODO
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

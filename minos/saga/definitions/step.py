"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

import uuid
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


class SagaStep(object):
    """TODO"""

    def __init__(
        self,
        saga: Optional[Saga] = None,
        raw_invoke_participant: dict[str, Any] = None,
        raw_with_compensation: dict[str, Any] = None,
        raw_on_reply: dict[str, Any] = None,
    ):
        self.saga = saga
        self.raw_invoke_participant = raw_invoke_participant
        self.raw_with_compensation = raw_with_compensation
        self.raw_on_reply = raw_on_reply

    @classmethod
    def from_raw(cls, raw: dict[str, Any], **kwargs) -> SagaStep:
        """TODO

        :param raw: TODO
        :param kwargs: TODO
        :return: TODO
        """
        if isinstance(raw, cls):
            return raw

        current = raw | kwargs
        return cls(**current)

    def invoke_participant(self, name: Union[str, list], callback: CallBack) -> SagaStep:
        """TODO

        :param name: TODO
        :param callback: TODO
        :return: TODO
        """
        if self.raw_invoke_participant is not None:
            raise MinosMultipleInvokeParticipantException()

        self.raw_invoke_participant = {"name": name, "callback": callback}

        return self

    def with_compensation(self, name: str, callback: CallBack) -> SagaStep:
        """TODO

        :param name: TODO
        :param callback: TODO
        :return: TODO
        """
        if self.raw_with_compensation is not None:
            raise MinosMultipleWithCompensationException()

        self.raw_with_compensation = {"name": name, "callback": callback}

        return self

    def on_reply(self, name: str, callback: Callable = identity_fn) -> SagaStep:
        """TODO

        :param name: TODO
        :param callback: TODO
        :return: TODO
        """
        if self.raw_on_reply is not None:
            raise MinosMultipleOnReplyException()

        self.raw_on_reply = {"name": name, "callback": callback}

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
        if self.raw_invoke_participant is None and self.raw_with_compensation is None and self.raw_on_reply is None:
            raise MinosSagaEmptyStepException()

        if self.raw_invoke_participant is None:
            raise MinosUndefinedInvokeParticipantException()

    @property
    def raw(self) -> dict[str, Any]:
        """TODO

        :return: TODO
        """
        return {
            "raw_invoke_participant": self.raw_invoke_participant,
            "raw_with_compensation": self.raw_with_compensation,
            "raw_on_reply": self.raw_on_reply,
        }

    def __eq__(self, other: SagaStep) -> bool:
        return type(self) == type(other) and tuple(self) == tuple(other)

    def __iter__(self) -> Iterable:
        yield from (
            self.raw_invoke_participant,
            self.raw_with_compensation,
            self.raw_on_reply,
        )

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
    from .saga import (
        Saga,
    )


class SagaStep(object):
    """TODO"""

    def __init__(self, saga: Optional[Saga] = None):
        self.saga = saga
        self.raw_invoke_participant = None
        self.raw_with_compensation = None
        self.raw_on_reply = None

    @property
    def raw(self) -> [dict[str, Any]]:
        """TODO

        :return: TODO
        """
        raw = list()
        if self.raw_invoke_participant is not None:
            raw.append(self.raw_invoke_participant)
        if self.raw_with_compensation is not None:
            raw.append(self.raw_with_compensation)
        if self.raw_on_reply is not None:
            raw.append(self.raw_on_reply)

        return raw

    def invoke_participant(self, name: str, callback: Callable = None) -> SagaStep:
        """TODO

        :param name: TODO
        :param callback: TODO
        :return: TODO
        """
        if self.raw_invoke_participant is not None:
            raise MinosMultipleInvokeParticipantException()

        self.raw_invoke_participant = {
            "id": str(uuid.uuid4()),
            "type": "invokeParticipant",
            "name": name,
            "callback": callback,
        }

        return self

    def with_compensation(self, name: Union[str, list], callback: Callable = None) -> SagaStep:
        """TODO

        :param name: TODO
        :param callback: TODO
        :return: TODO
        """
        if self.raw_with_compensation is not None:
            raise MinosMultipleWithCompensationException()

        self.raw_with_compensation = {
            "id": str(uuid.uuid4()),
            "type": "withCompensation",
            "name": name,
            "callback": callback,
        }

        return self

    def on_reply(self, _callback: Callable) -> SagaStep:
        """TODO

        :param _callback: TODO
        :return: TODO
        """
        if self.raw_on_reply is not None:
            raise MinosMultipleOnReplyException()

        self.raw_on_reply = {
            "id": str(uuid.uuid4()),
            "type": "onReply",
            "callback": _callback,
        }

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
        if self.saga is None:
            raise MinosSagaNotDefinedException()
        return self.saga

    def validate(self) -> NoReturn:
        """TODO

        :return TODO:
        """
        if not self.raw:
            raise MinosSagaEmptyStepException()

        if self.raw_invoke_participant is None:
            raise MinosUndefinedInvokeParticipantException()

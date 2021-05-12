"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

from enum import (
    Enum,
)
from typing import (
    Union,
)


class SagaStatus(Enum):
    """Saga Status class."""

    Created = "created"
    Running = "running"
    Paused = "paused"
    Finished = "finished"
    Errored = "errored"

    @classmethod
    def from_raw(cls, raw: Union[str, SagaStatus]) -> SagaStatus:
        """Build a new instance from raw.

        :param raw: The raw representation of the instance.
        :return: A ``SagaStatus`` instance.
        """
        if isinstance(raw, cls):
            return raw

        return next(obj for obj in cls if obj.value == raw)

    @property
    def raw(self) -> str:
        """Compute the raw representation of the instance.

        :return: A ``str`` instance.
        """
        return self.value


class SagaStepStatus(Enum):
    """Saga Step Status class."""

    Created = "created"
    RunningInvokeParticipant = "running-invoke-participant"
    FinishedInvokeParticipant = "finished-invoke-participant"
    ErroredInvokeParticipant = "errored"
    RunningWithCompensation = "running-with-compensation"
    PausedWithCompensation = "paused-with-compensation"
    ErroredWithCompensation = "errored-with-compensation"
    RunningOnReply = "running-on-reply"
    PausedOnReply = "paused-on-reply"
    ErroredOnReply = "errored-on-reply"
    Finished = "finished"

    @classmethod
    def from_raw(cls, raw: Union[str, SagaStepStatus]) -> SagaStepStatus:
        """Build a new instance from raw.

        :param raw: The raw representation of the instance.
        :return: A ``SagaStepStatus`` instance.
        """
        if isinstance(raw, cls):
            return raw

        return next(obj for obj in cls if obj.value == raw)

    @property
    def raw(self) -> str:
        """Compute the raw representation of the instance.

        :return: A ``str`` instance.
        """
        return self.value

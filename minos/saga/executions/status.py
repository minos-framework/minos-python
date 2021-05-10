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


class SagaStatus(Enum):
    """TODO"""

    Created = "created"
    Running = "running"
    Paused = "paused"
    Finished = "finished"
    Errored = "errored"

    @property
    def raw(self) -> str:
        """TODO

        :return: TODO
        """
        return self.value


class SagaStepStatus(Enum):
    """TODO"""

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

    @property
    def raw(self) -> str:
        """TODO

        :return: TODO
        """
        return self.value

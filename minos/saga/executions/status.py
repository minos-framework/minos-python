"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
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


class SagaStepStatus(Enum):
    """TODO"""

    Created = "created"
    RunningInvokeParticipant = "running-invoke-participant"
    PausedInvokeParticipant = "paused-invoke-participant"
    ErroredInvokeParticipant = "errored"
    RunningWithCompensation = "running-with-compensation"
    PausedWithCompensation = "paused-with-compensation"
    ErroredWithCompensation = "errored-with-compensation"
    RunningOnReply = "running-on-reply"
    ErroredOnReply = "errored-on-reply"
    Finished = "finished"

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
    RunningOnExecute = "running-on-execute"
    FinishedOnExecute = "finished-on-execute"
    ErroredOnExecute = "errored-on-execute"
    RunningOnFailure = "running-on-failure"
    PausedOnFailure = "paused-on-failure"
    ErroredOnFailure = "errored-on-failure"
    RunningOnSuccess = "running-on-success"
    PausedOnSuccess = "paused-on-success"
    ErroredOnSuccess = "errored-on-success"
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

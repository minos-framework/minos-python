"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from minos.common import (
    Aggregate,
    MinosException,
)


class MinosNetworkException(MinosException):
    """Base network exception."""


class MinosSnapshotException(MinosNetworkException):
    """Base snapshot exception"""


class MinosPreviousVersionSnapshotException(MinosSnapshotException):
    """Exception to be raised when current version is newer than the one to be processed."""

    def __init__(self, previous: Aggregate, new: Aggregate):
        self.previous = previous
        self.new = new
        super().__init__(
            f"Version for {repr(previous.classname)} aggregate must be "
            f"greater than {previous.version}. Obtained: {new.version}"
        )

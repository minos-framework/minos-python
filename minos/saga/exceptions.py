"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from minos.common import (
    MinosException,
)


class MinosSagaException(MinosException):
    """Base saga exception."""


class MinosSagaStepException(MinosSagaException):
    """TODO"""


class MinosMultipleInvokeParticipantException(MinosSagaStepException):
    """TODO"""

    def __init__(self, message: str = None):
        if message is None:
            message = "A 'SagaStep' can only define one 'invoke_participant' method."
        super().__init__(message)


class MinosMultipleWithCompensationException(MinosSagaStepException):
    """TODO"""

    def __init__(self, message: str = None):
        if message is None:
            message = "A 'SagaStep' can only define one 'with_compensation' method."
        super().__init__(message)


class MinosMultipleOnReplyException(MinosSagaStepException):
    """TODO"""

    def __init__(self, message: str = None):
        if message is None:
            message = "A 'SagaStep' can only define one 'on_reply' method."
        super().__init__(message)

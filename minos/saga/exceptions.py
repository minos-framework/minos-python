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


class MinosSagaNotDefinedException(MinosSagaStepException):
    """TODO"""

    def __init__(self, message: str = None):
        if message is None:
            message = "A 'SagaStep' must have a 'Saga' instance to call call this method."
        super().__init__(message)


class MinosSagaEmptyStepException(MinosSagaStepException):
    """TODO"""

    def __init__(self, message: str = None):
        if message is None:
            message = "A 'SagaStep' must have at least one defined action."
        super().__init__(message)


class MinosMultipleInvokeParticipantException(MinosSagaStepException):
    """TODO"""

    def __init__(self, message: str = None):
        if message is None:
            message = "A 'SagaStep' can only define one 'invoke_participant' method."
        super().__init__(message)


class MinosUndefinedInvokeParticipantCallbackException(MinosSagaStepException):
    """TODO"""

    def __init__(self, message: str = None):
        if message is None:
            message = "A 'SagaStep.invoke_participant' must have a data preparation callback."
        super().__init__(message)


class MinosMultipleWithCompensationException(MinosSagaStepException):
    """TODO"""

    def __init__(self, message: str = None):
        if message is None:
            message = "A 'SagaStep' can only define one 'with_compensation' method."
        super().__init__(message)


class MinosUndefinedWithCompensationCallbackException(MinosSagaStepException):
    """TODO"""

    def __init__(self, message: str = None):
        if message is None:
            message = "A 'SagaStep.with_compensation' must have a data preparation callback."
        super().__init__(message)


class MinosMultipleOnReplyException(MinosSagaStepException):
    """TODO"""

    def __init__(self, message: str = None):
        if message is None:
            message = "A 'SagaStep' can only define one 'on_reply' method."
        super().__init__(message)


class MinosAlreadyOnSagaException(MinosSagaStepException):
    """TODO"""

    def __init__(self, message: str = None):
        if message is None:
            message = "A 'SagaStep' can only belong to one 'Saga' simultaneously."
        super().__init__(message)


class MinosUndefinedInvokeParticipantException(MinosSagaStepException):
    """TODO"""

    def __init__(self, message: str = None):
        if message is None:
            message = "A 'SagaStep' must define at least the 'invoke_participant' logic."
        super().__init__(message)


class MinosSagaExecutionException(MinosSagaException):
    """TODO"""


class MinosSagaExecutionNotFoundException(MinosSagaExecutionException):
    """TODO"""


class MinosSagaRollbackExecutionException(MinosSagaExecutionException):
    """TODO"""


class MinosSagaExecutionStepException(MinosSagaException):
    """TODO"""


class MinosSagaFailedExecutionStepException(MinosSagaExecutionStepException):
    """TODO"""

    def __init__(self, message: str = None):
        if message is None:
            message = "There was a failure while 'SagaExecutionStep' was executing."
        super().__init__(message)


class MinosSagaPausedExecutionStepException(MinosSagaExecutionStepException):
    """TODO"""

    def __init__(self, message: str = None):
        if message is None:
            message = "There was a pause while 'SagaExecutionStep' was executing."
        super().__init__(message)


class MinosSagaRollbackExecutionStepException(MinosSagaExecutionStepException):
    """TODO"""

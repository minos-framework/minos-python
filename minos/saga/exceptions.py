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
    """Base exception for saga steps."""


class MinosSagaNotDefinedException(MinosSagaStepException):
    """Exception to be raised when the saga is not defined."""

    def __init__(self, message: str = None):
        if message is None:
            message = "A 'SagaStep' must have a 'Saga' instance to call call this method."
        super().__init__(message)


class MinosSagaEmptyStepException(MinosSagaStepException):
    """Exception to be raised when the step is empty."""

    def __init__(self, message: str = None):
        if message is None:
            message = "A 'SagaStep' must have at least one defined action."
        super().__init__(message)


class MinosMultipleInvokeParticipantException(MinosSagaStepException):
    """Exception to be raised when multiple invoke participant methods are defined."""

    def __init__(self, message: str = None):
        if message is None:
            message = "A 'SagaStep' can only define one 'invoke_participant' method."
        super().__init__(message)


class MinosMultipleWithCompensationException(MinosSagaStepException):
    """Exception to be raised when multiple with compensation methods are defined."""

    def __init__(self, message: str = None):
        if message is None:
            message = "A 'SagaStep' can only define one 'with_compensation' method."
        super().__init__(message)


class MinosMultipleOnReplyException(MinosSagaStepException):
    """Exception to be raised when multiple on reply methods are defined."""

    def __init__(self, message: str = None):
        if message is None:
            message = "A 'SagaStep' can only define one 'on_reply' method."
        super().__init__(message)


class MinosAlreadyOnSagaException(MinosSagaStepException):
    """Exception to be raised when a saga step is already in another saga."""

    def __init__(self, message: str = None):
        if message is None:
            message = "A 'SagaStep' can only belong to one 'Saga' simultaneously."
        super().__init__(message)


class MinosUndefinedInvokeParticipantException(MinosSagaStepException):
    """Exception to be raised when the invoke participant method is not defined."""

    def __init__(self, message: str = None):
        if message is None:
            message = "A 'SagaStep' must define at least the 'invoke_participant' logic."
        super().__init__(message)


class MinosSagaExecutionException(MinosSagaException):
    """Base exception for saga execution."""


class MinosSagaExecutionNotFoundException(MinosSagaExecutionException):
    """Exception to be raised when a saga execution is not found."""


class MinosSagaRollbackExecutionException(MinosSagaExecutionException):
    """Exception to be raised when a saga exception cannot be rollbacked"""


class MinosSagaNotCommittedException(MinosSagaExecutionException):
    """Exception to be raised when trying to exec a  not committed saga."""


class MinosSagaFailedExecutionException(MinosSagaExecutionException):
    """Exception to be raised when a saga execution failed while running."""

    def __init__(self, exception: Exception, message: str = None):
        self.exception = exception
        if message is None:
            message = f"There was a failure while 'SagaExecutionStep' was executing: {exception!r}"
        super().__init__(message)


class MinosSagaExecutionAlreadyExecutedException(MinosSagaExecutionException):
    """Exception to be raised when a saga execution cannot be executed."""


class MinosSagaExecutionStepException(MinosSagaException):
    """Base exception for saga execution step."""


class MinosSagaFailedExecutionStepException(MinosSagaExecutionStepException, MinosSagaFailedExecutionException):
    """Exception to be raised when a saga execution step failed while running."""


class MinosSagaPausedExecutionStepException(MinosSagaExecutionStepException):
    """Exception to be raised when a saga execution step is paused."""

    def __init__(self, message: str = None):
        if message is None:
            message = "There was a pause while 'SagaExecutionStep' was executing."
        super().__init__(message)


class MinosSagaRollbackExecutionStepException(MinosSagaExecutionStepException):
    """Exception to be raised when a saga execution step failed while performing a rollback."""


class MinosSagaAlreadyCommittedException(MinosSagaException):
    """Exception to be raised when trying to modifying an already committed saga."""


class MinosSagaExecutorException(MinosSagaException):
    """Exception to be raised when a saga executor raises some exception."""

    def __init__(self, exception: Exception, message: str = None):
        self.exception = exception
        if message is None:
            message = f"There was a failure while 'SagaExecutionStep' was executing: {exception!r}"
        super().__init__(message)


class MinosSagaFailedCommitCallbackException(MinosSagaFailedExecutionException):
    """Exception to be raised when a saga commit callback raises some exception"""


class MinosCommandReplyFailedException(MinosException):
    """Exception to be used when ``CommandStatus`` is not ``SUCCESS``"""

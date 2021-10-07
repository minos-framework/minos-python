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


class MinosMultipleOnExecuteException(MinosSagaStepException):
    """Exception to be raised when multiple on execute methods are defined."""

    def __init__(self, message: str = None):
        if message is None:
            message = "A 'SagaStep' can only define one 'on_execute' method."
        super().__init__(message)


class MinosMultipleOnFailureException(MinosSagaStepException):
    """Exception to be raised when multiple on failure methods are defined."""

    def __init__(self, message: str = None):
        if message is None:
            message = "A 'SagaStep' can only define one 'on_failure' method."
        super().__init__(message)


class MinosMultipleOnSuccessException(MinosSagaStepException):
    """Exception to be raised when multiple on success methods are defined."""

    def __init__(self, message: str = None):
        if message is None:
            message = "A 'SagaStep' can only define one 'on_success' method."
        super().__init__(message)


class MinosAlreadyOnSagaException(MinosSagaStepException):
    """Exception to be raised when a saga step is already in another saga."""

    def __init__(self, message: str = None):
        if message is None:
            message = "A 'SagaStep' can only belong to one 'Saga' simultaneously."
        super().__init__(message)


class MinosUndefinedOnExecuteException(MinosSagaStepException):
    """Exception to be raised when the on execute method is not defined."""

    def __init__(self, message: str = None):
        if message is None:
            message = "A 'SagaStep' must define at least the 'on_execute' logic."
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
            message = f"There was a failure while 'SagaStepExecution' was executing: {exception!r}"
        super().__init__(message)


class MinosSagaExecutionAlreadyExecutedException(MinosSagaExecutionException):
    """Exception to be raised when a saga execution cannot be executed."""


class MinosSagaStepExecutionException(MinosSagaException):
    """Base exception for saga execution step."""


class MinosSagaFailedExecutionStepException(MinosSagaStepExecutionException, MinosSagaFailedExecutionException):
    """Exception to be raised when a saga execution step failed while running."""


class MinosSagaPausedExecutionStepException(MinosSagaStepExecutionException):
    """Exception to be raised when a saga execution step is paused."""

    def __init__(self, message: str = None):
        if message is None:
            message = "There was a pause while 'SagaStepExecution' was executing."
        super().__init__(message)


class MinosSagaRollbackExecutionStepException(MinosSagaStepExecutionException):
    """Exception to be raised when a saga execution step failed while performing a rollback."""


class MinosSagaAlreadyCommittedException(MinosSagaException):
    """Exception to be raised when trying to modifying an already committed saga."""


class MinosSagaExecutorException(MinosSagaException):
    """Exception to be raised when a saga executor raises some exception."""

    def __init__(self, exception: Exception, message: str = None):
        self.exception = exception
        if message is None:
            message = f"There was a failure while 'SagaStepExecution' was executing: {exception!r}"
        super().__init__(message)


class MinosSagaFailedCommitCallbackException(MinosSagaFailedExecutionException):
    """Exception to be raised when a saga commit callback raises some exception"""


class MinosCommandReplyFailedException(MinosException):
    """Exception to be used when ``CommandStatus`` is not ``SUCCESS``"""

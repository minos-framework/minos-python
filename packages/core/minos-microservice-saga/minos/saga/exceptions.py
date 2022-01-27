from minos.common import (
    MinosException,
)


class SagaException(MinosException):
    """Base saga exception."""


class EmptySagaException(SagaException):
    """Exception to be raised when saga is empty."""

    def __init__(self, message: str = None):
        if message is None:
            message = "A 'Saga' must have at least one step."
        super().__init__(message)


class SagaNotCommittedException(SagaException):
    """Exception to be raised when trying to exec a  not committed saga."""

    def __init__(self, message: str = None):
        if message is None:
            message = "A 'Saga' must be committed."

        super().__init__(message)


class SagaStepException(SagaException):
    """Base exception for saga steps."""


class SagaNotDefinedException(SagaStepException):
    """Exception to be raised when the saga is not defined."""

    def __init__(self, message: str = None):
        if message is None:
            message = "A 'SagaStep' must have a 'Saga' instance to call call this method."
        super().__init__(message)


class EmptySagaStepException(SagaStepException):
    """Exception to be raised when the step is empty."""

    def __init__(self, message: str = None):
        if message is None:
            message = "A 'SagaStep' must have at least one defined action."
        super().__init__(message)


class MultipleOnExecuteException(SagaStepException):
    """Exception to be raised when multiple on execute methods are defined."""

    def __init__(self, message: str = None):
        if message is None:
            message = "A 'SagaStep' can only define one 'on_execute' method."
        super().__init__(message)


class MultipleOnFailureException(SagaStepException):
    """Exception to be raised when multiple on failure methods are defined."""

    def __init__(self, message: str = None):
        if message is None:
            message = "A 'SagaStep' can only define one 'on_failure' method."
        super().__init__(message)


class MultipleOnSuccessException(SagaStepException):
    """Exception to be raised when multiple on success methods are defined."""

    def __init__(self, message: str = None):
        if message is None:
            message = "A 'SagaStep' can only define one 'on_success' method."
        super().__init__(message)


class MultipleOnErrorException(SagaStepException):
    """Exception to be raised when multiple on error methods are defined."""

    def __init__(self, message: str = None):
        if message is None:
            message = "A 'SagaStep' can only define one 'on_error' method."
        super().__init__(message)


class MultipleElseThenException(SagaStepException):
    """Exception to be raised when multiple else then alternatives are defined."""

    def __init__(self, message: str = None):
        if message is None:
            message = "A 'ConditionalSagaStep' can only define one 'else_then' method."
        super().__init__(message)


class AlreadyOnSagaException(SagaStepException):
    """Exception to be raised when a saga step is already in another saga."""

    def __init__(self, message: str = None):
        if message is None:
            message = "A 'SagaStep' can only belong to one 'Saga' simultaneously."
        super().__init__(message)


class UndefinedOnExecuteException(SagaStepException):
    """Exception to be raised when the on execute method is not defined."""

    def __init__(self, message: str = None):
        if message is None:
            message = "A 'SagaStep' must define at least the 'on_execute' logic."
        super().__init__(message)


class SagaExecutionException(SagaException):
    """Base exception for saga execution."""


class SagaExecutionNotFoundException(SagaExecutionException):
    """Exception to be raised when a saga execution is not found."""


class SagaRollbackExecutionException(SagaExecutionException):
    """Exception to be raised when a saga exception cannot be rollbacked"""


class SagaFailedExecutionException(SagaExecutionException):
    """Exception to be raised when a saga execution failed while running."""

    def __init__(self, exception: Exception, message: str = None):
        self.exception = exception
        if message is None:
            message = f"There was a failure while 'SagaStepExecution' was executing: {exception!r}"
        super().__init__(message)


class SagaExecutionAlreadyExecutedException(SagaExecutionException):
    """Exception to be raised when a saga execution cannot be executed."""


class SagaStepExecutionException(SagaException):
    """Base exception for saga execution step."""


class SagaFailedExecutionStepException(SagaStepExecutionException, SagaFailedExecutionException):
    """Exception to be raised when a saga execution step failed while running."""


class SagaPausedExecutionStepException(SagaStepExecutionException):
    """Exception to be raised when a saga execution step is paused."""

    def __init__(self, message: str = None):
        if message is None:
            message = "There was a pause while 'SagaStepExecution' was executing."
        super().__init__(message)


class SagaRollbackExecutionStepException(SagaStepExecutionException):
    """Exception to be raised when a saga execution step failed while performing a rollback."""


class AlreadyCommittedException(SagaException):
    """Exception to be raised when trying to modifying an already committed saga."""


class ExecutorException(SagaException):
    """Exception to be raised when a saga executor raises some exception."""

    def __init__(self, exception: Exception, message: str = None):
        self.exception = exception
        if message is None:
            message = f"There was a failure while 'SagaStepExecution' was executing: {exception!r}"
        super().__init__(message)


class SagaFailedCommitCallbackException(SagaFailedExecutionException):
    """Exception to be raised when a saga commit callback raises some exception"""


class SagaResponseException(SagaException):
    """Exception to be used when ``CommandStatus`` is not ``SUCCESS``"""

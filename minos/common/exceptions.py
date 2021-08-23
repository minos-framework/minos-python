"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

from typing import (
    TYPE_CHECKING,
    Any,
    Type,
)

if TYPE_CHECKING:
    from .model import (
        Aggregate,
        AggregateDiff,
    )


class MinosException(Exception):
    """Exception class for import packages or modules"""

    __slots__ = "_message"

    def __init__(self, error_message: str):
        self._message = error_message

    def __repr__(self):
        return f"{type(self).__name__}(message={repr(self._message)})"

    def __str__(self) -> str:
        """represent in a string format the error message passed during the instantiation"""
        return self._message


class MinosImportException(MinosException):
    pass


class MinosProtocolException(MinosException):
    pass


class MinosMessageException(MinosException):
    pass


class MinosConfigException(MinosException):
    """Base config exception."""


class MinosConfigNotProvidedException(MinosConfigException):
    """Exception to be raised when a config is needed but none is set."""


class MinosBrokerException(MinosException):
    """Base broker exception"""


class MinosBrokerNotProvidedException(MinosBrokerException):
    """Exception to be raised when a broker is needed but none is set."""


class MinosHandlerException(MinosException):
    """Base handler exception"""


class MinosHandlerNotProvidedException(MinosHandlerException):
    """Exception to be raised when a handler is needed but none is set."""


class MinosRepositoryException(MinosException):
    """Base repository exception."""


class MinosRepositoryNotProvidedException(MinosRepositoryException):
    """Exception to be raised when a repository is needed but none is set."""


class MinosSnapshotException(MinosException):
    """Base snapshot exception."""


class MinosSnapshotNotProvidedException(MinosSnapshotException):
    """Exception to be raised when a snapshot is needed but none is set."""


class MinosPreviousVersionSnapshotException(MinosSnapshotException):
    """Exception to be raised when current version is newer than the one to be processed."""

    def __init__(self, previous: Aggregate, aggregate_diff: AggregateDiff):
        self.previous = previous
        self.aggregate_diff = aggregate_diff
        super().__init__(
            f"Version for {repr(previous.classname)} aggregate must be "
            f"greater than {previous.version}. Obtained: {aggregate_diff.version}"
        )


class MinosSnapshotAggregateNotFoundException(MinosSnapshotException):
    """Exception to be raised when some aggregate is not found on the repository."""


class MinosSnapshotDeletedAggregateException(MinosSnapshotException):
    """Exception to be raised when some aggregate is already deleted from the repository."""


class MinosSagaManagerException(MinosException):
    """Base Saga Manager exception"""


class MinosSagaManagerNotProvidedException(MinosSagaManagerException):
    """Exception to be raised when a SAGA Manager is not provided."""


class MinosModelException(MinosException):
    """Exception to be raised when some mandatory condition is not satisfied by a model."""

    pass


class EmptyMinosModelSequenceException(MinosModelException):
    """Exception to be raised when a sequence must be not empty, but it is empty."""

    pass


class MultiTypeMinosModelSequenceException(MinosModelException):
    """Exception to be raised when a sequence doesn't satisfy the condition to have the same type for each item."""

    pass


class MinosModelAttributeException(MinosException):
    """Base model attributes exception."""

    pass


class MinosImmutableClassException(MinosException):
    """If an attribute of an immutable class is modified, this exception will be raised"""

    pass


class MinosReqAttributeException(MinosModelAttributeException):
    """Exception to be raised when some required attributes are not provided."""

    pass


class MinosTypeAttributeException(MinosModelAttributeException):
    """Exception to be raised when there are any mismatching between the expected and observed attribute type."""

    def __init__(self, name: str, target_type: Type, value: Any):
        self.name = name
        self.target_type = target_type
        self.value = value
        super().__init__(
            f"The {target_type!r} expected type for {name!r} does not match with "
            f"the given data type: {type(value)!r} ({value!r})"
        )


class MinosMalformedAttributeException(MinosModelAttributeException):
    """Exception to be raised when there are any kind of problems with the type definition."""

    pass


class MinosParseAttributeException(MinosModelAttributeException):
    """Exception to be raised when there are any kind of problems with the parsing logic."""

    def __init__(self, name: str, value: Any, exception: Exception):
        self.name = name
        self.value = value
        self.exception = exception
        super().__init__(f"{repr(exception)} was raised while parsing {repr(name)} field with {repr(value)} value.")


class MinosAttributeValidationException(MinosModelAttributeException):
    """Exception to be raised when some fields are not valid."""

    def __init__(self, name: str, value: Any):
        self.name = name
        self.value = value
        super().__init__(f"{repr(value)} value does not pass the {repr(name)} field validation.")

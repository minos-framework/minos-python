from __future__ import (
    annotations,
)

from typing import (
    Any,
    Type,
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


class NotProvidedException(MinosException):
    """Exception to be raised when a dependency is needed but not provided."""


class MinosImportException(MinosException):
    pass


class MinosProtocolException(MinosException):
    pass


class MinosMessageException(MinosException):
    pass


class MinosConfigException(MinosException):
    """Base config exception."""


class MinosBrokerException(MinosException):
    """Base broker exception"""


class MinosHandlerException(MinosException):
    """Base handler exception"""


class MinosLockException(MinosException):
    """Base lock exception"""


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


class DataDecoderException(MinosModelException):
    """Base data decoder exception."""


class DataDecoderMalformedTypeException(DataDecoderException):
    """Exception to be raised when malformed types are provided."""


class DataDecoderRequiredValueException(DataDecoderException):
    """Exception to be raised when required values are not provided."""


class DataDecoderTypeException(DataDecoderException):
    """Exception to be raised when expected and provided types do not match."""

    def __init__(self, target_type: Type, value: Any):
        self.target_type = target_type
        self.value = value
        super().__init__(
            f"The {target_type!r} expected type does not match the given data type: {type(value)!r} ({value!r})"
        )

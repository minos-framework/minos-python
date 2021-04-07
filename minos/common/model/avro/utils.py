import typing as t

from minos.common.model.types import CUSTOM_TYPES


def is_union(a_type: t.Any) -> bool:
    """
    Given a python type, return True if is typing.Union, otherwise False
    Arguments:
        a_type (typing.Any): python type
    Returns:
        bool
    """
    return isinstance(a_type, t._GenericAlias) and a_type.__origin__ is t.Union  # type: ignore


def is_custom_type(value: t.Any) -> bool:
    """
    Given a type, return True if is a custom type (Fixed, Enum)
    """
    return isinstance(value, dict) and value.get("_dataclasses_custom_type") in CUSTOM_TYPES

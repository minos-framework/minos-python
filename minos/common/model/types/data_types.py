"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from typing import (
    Generic,
    TypeVar,
)

T = TypeVar("T")
NoneType = type(None)


class MissingSentinel(Generic[T]):
    """Class to detect when a field is not initialized."""

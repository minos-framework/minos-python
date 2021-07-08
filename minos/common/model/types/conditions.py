"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from typing import (
    Any,
)


def is_model_subclass(type_field: Any) -> bool:
    """Check if the given type field is subclass of ``Model``."""
    from ..abc import (
        Model,
    )

    return issubclass(type_field, Model)


def is_aggregate_subclass(type_field: Any) -> bool:
    """Check if the given type field is subclass of ``Aggregate``."""
    from ..declarative import (
        Aggregate,
    )

    return issubclass(type_field, Aggregate)


def is_type_subclass(type_field: Any) -> bool:
    """Check if the given type field is subclass of ``type``."""
    return issubclass(type(type_field), type(type))

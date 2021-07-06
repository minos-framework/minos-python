"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from typing import (
    Type,
)


def _is_model_cls(type_field: Type) -> bool:
    from ..abc import (
        Model,
    )

    return issubclass(type_field, Model)


def _is_aggregate_cls(type_field: Type) -> bool:
    from ..declarative import (
        Aggregate,
    )

    return issubclass(type_field, Aggregate)


def _is_type(type_field) -> bool:
    return issubclass(type(type_field), type(type))

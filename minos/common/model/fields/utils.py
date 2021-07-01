"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import typing as t


def _is_model_cls(type_field: t.Type) -> bool:
    from ..abc import (
        Model,
    )

    return issubclass(type(type_field), type(type)) and issubclass(type_field, Model)


def _is_aggregate_cls(type_field: t.Type) -> bool:
    from ..declarative import (
        Aggregate,
    )

    return issubclass(type(type_field), type(type)) and issubclass(type_field, Aggregate)

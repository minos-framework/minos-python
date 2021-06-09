"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import inspect
import typing as t


def _is_minos_model_cls(type_field: t.Type) -> bool:
    from ..model import (
        MinosModel,
    )

    return inspect.isclass(type_field) and issubclass(type_field, MinosModel)


def _is_aggregate_cls(type_field: t.Type) -> bool:
    from ...aggregate import (
        Aggregate,
    )

    return inspect.isclass(type_field) and issubclass(type_field, Aggregate)

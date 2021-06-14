"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

import typing as t

from ..fields import (
    ModelField,
)
from .abc import (
    DynamicModel,
)


class DataTransferObject(DynamicModel):
    """Data Transfer Object to build the objects dynamically from bytes """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @classmethod
    def from_typed_dict(cls, typed_dict: t.TypedDict, data: dict[str, t.Any]) -> DataTransferObject:
        """Build Model Fields form TypeDict object

        :param typed_dict: TypeDict object
        :param data: Data
        :return: DataTransferObject
        """
        fields = dict()
        for name, type_val in typed_dict.__annotations__.items():
            fields[name] = ModelField(name, type_val, data[name])
        return cls(fields)

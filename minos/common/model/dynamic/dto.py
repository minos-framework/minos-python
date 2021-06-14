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

    def __init__(self, name: str, namespace: str = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if namespace is None:
            namespace, name = name.rsplit(".", 1)
        self._name = name
        self._namespace = namespace

    @classmethod
    def from_typed_dict(cls, typed_dict: t.TypedDict, data: dict[str, t.Any]) -> DataTransferObject:
        """Build Model Fields form TypeDict object

        :param typed_dict: TypeDict object
        :param data: Data
        :return: DataTransferObject
        """
        namespace, name = typed_dict.__name__.rsplit(".", 1)
        fields = dict()
        for name, type_val in typed_dict.__annotations__.items():
            fields[name] = ModelField(name, type_val, data[name])
        return cls(name, namespace, fields)

    # noinspection PyMethodParameters
    @property
    def _avro_name(self):
        return self._name

    @property
    def _avro_namespace(self):
        return self._namespace

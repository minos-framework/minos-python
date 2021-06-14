"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

from typing import (
    Any,
    TypedDict,
    Union,
)

from ...exceptions import (
    MinosImportException,
)
from ...importlib import (
    import_module,
)
from .. import (
    Model,
)
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
            try:
                namespace, name = name.rsplit(".", 1)
            except ValueError:
                pass
        self._name = name
        self._namespace = namespace

    @classmethod
    def from_avro(cls, schema: Union[dict[str, Any], list[dict[str, Any]]], data: dict[str, Any]):
        """Build a new instance from the ``avro`` schema and data.

        :param schema: The avro schema of the model.
        :param data: The avro data of the model.
        :return: A new ``DynamicModel`` instance.
        """
        if isinstance(schema, list):
            schema = schema[-1]

        if "namespace" in schema:
            name = "{namespace:}.{name:}".format(**schema)
        else:
            name = schema["name"]

        try:
            # noinspection PyTypeChecker
            model_cls: Model = import_module(name)
        except MinosImportException:
            return super().from_avro(schema, data)

        return model_cls.from_dict(data)

    @classmethod
    def from_typed_dict(cls, typed_dict: TypedDict, data: dict[str, Any]) -> DataTransferObject:
        """Build Model Fields form TypeDict object

        :param typed_dict: TypeDict object
        :param data: Data
        :return: DataTransferObject
        """

        try:
            # noinspection PyTypeChecker
            model_cls: Model = import_module(typed_dict.__name__)
            return model_cls.from_dict(data)
        except MinosImportException:
            pass

        try:
            namespace, name = typed_dict.__name__.rsplit(".", 1)
        except ValueError:
            namespace, name = None, typed_dict.__name__
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

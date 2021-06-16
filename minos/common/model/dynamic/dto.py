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
    Optional,
    TypedDict,
    Union,
)

from ...exceptions import (
    MinosImportException,
    MinosModelException,
)
from ...importlib import (
    import_module,
)
from ..abc import (
    Model,
)
from ..fields import (
    ModelField,
)
from ..types import (
    ModelType,
)
from .abc import (
    DynamicModel,
)


class DataTransferObject(DynamicModel):
    """Data Transfer Object to build the objects dynamically from bytes """

    def __init__(self, name: str, namespace: Optional[str] = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if namespace is None:
            try:
                namespace, name = name.rsplit(".", 1)
            except ValueError:
                namespace = str()
        self._name = name
        self._namespace = namespace

    @classmethod
    def from_avro(cls, schema: Union[dict[str, Any], list[dict[str, Any]]], data: dict[str, Any]) -> Model:
        """Build a new instance from the ``avro`` schema and data.

        :param schema: The avro schema of the model.
        :param data: The avro data of the model.
        :return: A new ``DynamicModel`` instance.
        """
        if isinstance(schema, list):
            schema = schema[-1]

        if "namespace" in schema and len(schema["namespace"]) > 0:
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
        """Build a ``DataTransferObject`` from a ``TypeDict`` and ``data``.

        :param typed_dict: ``TypeDict`` object containing the DTO's structure
        :param data: A dictionary containing the values to be stored on the DTO.
        :return: A new ``DataTransferObject`` instance.
        """
        return cls.from_model_type(ModelType.from_typed_dict(typed_dict), data)

    @classmethod
    def from_model_type(cls, model_type: ModelType, data: dict[str, Any]) -> DataTransferObject:
        """Build a ``DataTransferObject`` from a ``ModelType`` and ``data``.

        :param model_type: ``ModelType`` object containing the DTO's structure
        :param data: A dictionary containing the values to be stored on the DTO.
        :return: A new ``DataTransferObject`` instance.
        """

        try:
            # noinspection PyTypeChecker
            model_cls: Model = import_module(model_type.classname)
            if model_cls.type_hints != model_type.type_hints:
                raise MinosModelException(f"The typed dict fields do not match with the {model_cls!r} fields")
            return model_cls.from_dict(data)
        except MinosImportException:
            pass

        fields = {k: ModelField(k, v, data[k]) for k, v in model_type.type_hints.items()}
        return cls(model_type.name, model_type.namespace, fields)

    # noinspection PyMethodParameters
    @property
    def _avro_name(self) -> str:
        return self._name

    @property
    def _avro_namespace(self) -> Optional[str]:
        return self._namespace

    @property
    def classname(self) -> str:
        """Compute the current class name.

        :return: An string object.
        """
        name = self._name
        if len(self._namespace) > 0:
            name = f"{self._namespace}.{name}"
        return name

    def __repr__(self) -> str:
        fields_repr = ", ".join(repr(field) for field in self.fields.values())
        return f"{self.classname}[DTO](fields=[{fields_repr}])"

    def __eq__(self, other: DataTransferObject):
        return (
            super(DynamicModel, self).__eq__(other)
            and self._name == other._name
            and self._namespace == other._namespace
        )

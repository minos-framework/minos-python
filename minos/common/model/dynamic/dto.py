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
)

from ..fields import (
    Field,
)
from ..types import (
    ModelType,
)
from .abc import (
    DynamicModel,
)


class DataTransferObject(DynamicModel):
    """Data Transfer Object to build the objects dynamically from bytes """

    def __init__(self, name: str, *args, namespace: str = str(), **kwargs):
        super().__init__(*args, **kwargs)
        if "." in name:
            raise ValueError(
                "The 'name' attribute cannot contain dots. You can use the 'namespace' to qualify the instance"
            )
        self._name = name
        self._namespace = namespace

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
        fields = {k: Field(k, v, data[k]) for k, v in model_type.type_hints.items()}
        return cls(model_type.name, fields, namespace=model_type.namespace)

    @property
    def classname(self) -> str:
        """Compute the current class name.

        :return: An string object.
        """
        if len(self._namespace) == 0:
            return self._name
        return f"{self._namespace}.{self._name}"

    def __repr__(self) -> str:
        fields_repr = ", ".join(repr(field) for field in self.fields.values())
        return f"{self._name}[DTO]({fields_repr})"

    def __eq__(self, other: DataTransferObject):
        return (
            super(DynamicModel, self).__eq__(other)
            and self._name == other._name
            and self._namespace == other._namespace
        )

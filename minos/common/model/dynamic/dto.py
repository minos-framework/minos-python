"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
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
    def from_model_type(cls, model_type: ModelType, *args, **kwargs) -> DataTransferObject:
        """Build a ``DataTransferObject`` from a ``ModelType``.

        :param model_type: ``ModelType`` object containing the model structure
        :param args: Positional arguments to be passed to the model constructor.
        :param kwargs: Named arguments to be passed to the model constructor.
        :return: A new ``DataTransferObject`` instance.
        """
        fields = cls._build_fields(model_type.type_hints, *args, **kwargs)
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

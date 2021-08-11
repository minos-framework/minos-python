"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

import logging
from itertools import (
    zip_longest,
)
from typing import (
    Iterable,
    Type,
    TypeVar,
    Union,
)

from ..abc import (
    Model,
)
from ..fields import (
    Field,
)
from ..types import (
    MissingSentinel,
    ModelType,
)

logger = logging.getLogger(__name__)


class DynamicModel(Model):
    """Base class for ``minos`` dynamic model entities"""

    def __init__(self, fields: Union[Iterable[Field], dict[str, Field]], **kwargs):
        super().__init__(fields)

    @classmethod
    def from_model_type(cls: Type[T], model_type: ModelType, *args, **kwargs) -> T:
        """Build a ``DynamicModel`` from a ``ModelType``.

        :param model_type: ``ModelType`` object containing the model structure
        :param args: Positional arguments to be passed to the model constructor.
        :param kwargs: Named arguments to be passed to the model constructor.
        :return: A new ``DynamicModel`` instance.
        """
        fields = cls._build_fields(model_type.type_hints, *args, **kwargs)
        return cls(fields=fields)

    @staticmethod
    def _build_fields(type_hints: dict[str, type], *args, **kwargs) -> dict[str, Field]:
        fields = dict()
        for (name, type_val), value in zip_longest(type_hints.items(), args, fillvalue=MissingSentinel):
            if name in kwargs and value is not MissingSentinel:
                raise TypeError(f"got multiple values for argument {repr(name)}")

            if value is MissingSentinel and name in kwargs:
                value = kwargs[name]

            fields[name] = Field(name, type_val, value)
        return fields


T = TypeVar("T", bound=DynamicModel)

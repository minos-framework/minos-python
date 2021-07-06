"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

import logging
from typing import (
    Any,
    Generic,
    Iterable,
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
    ModelType,
)

logger = logging.getLogger(__name__)

T = TypeVar("T")


class DynamicModel(Model, Generic[T]):
    """Base class for ``minos`` dynamic model entities"""

    def __init__(self, fields: Union[Iterable[Field], dict[str, Field]], **kwargs):
        super().__init__(fields)

    @classmethod
    def from_model_type(cls, model_type: ModelType, data: dict[str, Any]) -> T:
        """Build a ``DynamicModel`` from a ``ModelType`` and ``data``.

        :param model_type: ``ModelType`` object containing the DTO's structure
        :param data: A dictionary containing the values to be stored on the DTO.
        :return: A new ``DynamicModel`` instance.
        """
        return cls(fields={k: Field(k, v, data[k]) for k, v in model_type.type_hints.items()})

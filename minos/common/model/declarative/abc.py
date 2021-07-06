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
    Any,
    Generic,
    NoReturn,
    TypeVar,
    get_type_hints,
)

from ...meta import (
    self_or_classmethod,
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

T = TypeVar("T")


class DeclarativeModel(Model, Generic[T]):
    """Base class for ``minos`` declarative model entities."""

    def __init__(self, *args, **kwargs):
        """Class constructor.

        :param kwargs: Named arguments to be set as model attributes.
        """
        super().__init__()
        self._list_fields(*args, **kwargs)

    # noinspection PyUnusedLocal
    @classmethod
    def from_model_type(cls, model_type: ModelType, data: dict[str, Any]):
        """Build a ``DeclarativeModel`` from a ``ModelType`` and ``data``.

        :param model_type: ``ModelType`` object containing the DTO's structure
        :param data: A dictionary containing the values to be stored on the DTO.
        :return: A new ``DeclarativeModel`` instance.
        """
        return cls(**data)

    def _list_fields(self, *args, **kwargs) -> NoReturn:
        for (name, type_val), value in zip_longest(self._type_hints(), args, fillvalue=MissingSentinel):
            if name in kwargs and value is not MissingSentinel:
                raise TypeError(f"got multiple values for argument {repr(name)}")

            if value is MissingSentinel and name in kwargs:
                value = kwargs[name]

            self._fields[name] = Field(
                name, type_val, value, getattr(self, f"parse_{name}", None), getattr(self, f"validate_{name}", None)
            )

    # noinspection PyMethodParameters
    @self_or_classmethod
    def _type_hints(self_or_cls) -> dict[str, Any]:
        fields = dict()
        if isinstance(self_or_cls, type):
            cls = self_or_cls
        else:
            cls = type(self_or_cls)
        for b in cls.__mro__[::-1]:
            base_fields = getattr(b, "_fields", None)
            if base_fields is not None:
                list_fields = {k: v for k, v in get_type_hints(b).items() if not k.startswith("_")}
                fields |= list_fields
        logger.debug(f"The obtained fields are: {fields!r}")
        fields |= super()._type_hints()
        yield from fields.items()


MinosModel = DeclarativeModel

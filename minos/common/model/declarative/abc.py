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
    Iterator,
    NoReturn,
    Type,
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


class DeclarativeModel(Model):
    """Base class for ``minos`` declarative model entities."""

    def __init__(self, *args, **kwargs):
        """Class constructor.

        :param kwargs: Named arguments to be set as model attributes.
        """
        super().__init__()
        self._build_fields(*args, **kwargs)

    # noinspection PyUnusedLocal
    @classmethod
    def from_model_type(cls: Type[T], model_type: ModelType, *args, **kwargs) -> T:
        """Build a ``DeclarativeModel`` from a ``ModelType``.

        :param model_type: ``ModelType`` object containing the model structure.
        :param args: Positional arguments to be passed to the model constructor.
        :param kwargs: Named arguments to be passed to the model constructor.
        :return: A new ``DeclarativeModel`` instance.
        """
        return cls(*args, **kwargs)

    def _build_fields(self, *args, **kwargs) -> NoReturn:
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
    def _type_hints(self_or_cls) -> Iterator[tuple[str, Any]]:
        fields = dict()
        if isinstance(self_or_cls, type):
            cls = self_or_cls
        else:
            cls = type(self_or_cls)
        for b in cls.__mro__[::-1]:
            list_fields = {k: v for k, v in get_type_hints(b).items() if not k.startswith("_")}
            fields |= list_fields
        logger.debug(f"The obtained fields are: {fields!r}")
        fields |= super()._type_hints()
        yield from fields.items()


T = TypeVar("T", bound=DeclarativeModel)
MinosModel = DeclarativeModel

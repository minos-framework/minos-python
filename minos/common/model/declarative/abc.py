"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

import logging
import typing as t
from itertools import (
    zip_longest,
)

from ...importlib import (
    classname,
)
from ...meta import (
    classproperty,
    self_or_classmethod,
)
from ...protocol import (
    MinosAvroProtocol,
)
from ..abc import (
    Model,
)
from ..fields import (
    ModelField,
)
from ..types import (
    MissingSentinel,
)

logger = logging.getLogger(__name__)

T = t.TypeVar("T")


class DeclarativeModel(Model, t.Generic[T]):
    """Base class for ``minos`` model entities."""

    def __init__(self, *args, **kwargs):
        """Class constructor.

        :param kwargs: Named arguments to be set as model attributes.
        """
        super().__init__()
        self._list_fields(*args, **kwargs)

    @classmethod
    def from_avro_bytes(cls, raw: bytes, **kwargs) -> t.Union[T, list[T]]:
        """Build a single instance or a sequence of instances from bytes

        :param raw: A bytes data.
        :return: A single instance or a sequence of instances.
        """

        decoded = MinosAvroProtocol().decode(raw)
        if isinstance(decoded, list):
            return [cls.from_dict(d | kwargs) for d in decoded]
        return cls.from_dict(decoded | kwargs)

    @classmethod
    def from_dict(cls, d: dict[str, t.Any]) -> T:
        """Build a new instance from a dictionary.

        :param d: A dictionary object.
        :return: A new ``MinosModel`` instance.
        """
        return cls(**d)

    # noinspection PyMethodParameters
    @classproperty
    def classname(cls) -> str:
        """Compute the current class namespace.

        :return: An string object.
        """
        # noinspection PyTypeChecker
        return classname(cls)

    def _list_fields(self, *args, **kwargs) -> t.NoReturn:
        for (name, type_val), value in zip_longest(self._type_hints(), args, fillvalue=MissingSentinel):
            if name in kwargs and value is not MissingSentinel:
                raise TypeError(f"got multiple values for argument {repr(name)}")

            if value is MissingSentinel and name in kwargs:
                value = kwargs[name]

            self._fields[name] = ModelField(
                name, type_val, value, getattr(self, f"parse_{name}", None), getattr(self, f"validate_{name}", None)
            )

    # noinspection PyMethodParameters
    @self_or_classmethod
    def _type_hints(self_or_cls) -> dict[str, t.Any]:
        fields = dict()
        if isinstance(self_or_cls, type):
            cls = self_or_cls
        else:
            cls = type(self_or_cls)
        for b in cls.__mro__[::-1]:
            base_fields = getattr(b, "_fields", None)
            if base_fields is not None:
                list_fields = {k: v for k, v in t.get_type_hints(b).items() if not k.startswith("_")}
                fields |= list_fields
        logger.debug(f"The obtained fields are: {fields!r}")
        yield from fields.items()


MinosModel = DeclarativeModel

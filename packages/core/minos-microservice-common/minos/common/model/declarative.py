from __future__ import (
    annotations,
)

import logging
from collections import (
    ChainMap,
)
from functools import (
    lru_cache,
)
from itertools import (
    zip_longest,
)
from typing import (
    Any,
    Iterator,
    Optional,
    TypeVar,
    get_type_hints,
)

from ..meta import (
    self_or_classmethod,
)
from .abc import (
    Model,
)
from .types import (
    MissingSentinel,
    ModelType,
    TypeHintComparator,
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
    def from_model_type(cls: type[T], model_type: ModelType, *args, **kwargs) -> T:
        """Build a ``DeclarativeModel`` from a ``ModelType``.

        :param model_type: ``ModelType`` object containing the model structure.
        :param args: Positional arguments to be passed to the model constructor.
        :param kwargs: Named arguments to be passed to the model constructor.
        :return: A new ``DeclarativeModel`` instance.
        """
        return cls(*args, **kwargs)

    def _build_fields(self, *args, additional_type_hints: Optional[dict[str, type]] = None, **kwargs) -> None:
        iterable = zip_longest(self._type_hints(additional_type_hints), args, fillvalue=MissingSentinel)
        for (name, type_val), value in iterable:
            if name in kwargs and value is not MissingSentinel:
                raise TypeError(f"got multiple values for argument {repr(name)}")

            if value is MissingSentinel and name in kwargs:
                value = kwargs[name]

            self._fields[name] = self._field_cls(
                name, type_val, value, getattr(self, f"parse_{name}", None), getattr(self, f"validate_{name}", None)
            )

    # noinspection PyMethodParameters
    @self_or_classmethod
    def _type_hints(self_or_cls, additional_type_hints: Optional[dict[str, type]] = None) -> Iterator[tuple[str, Any]]:
        if isinstance(self_or_cls, type):
            cls = self_or_cls
        else:
            cls = type(self_or_cls)

        base = _get_class_type_hints(cls)

        additional = dict()
        if additional_type_hints:
            for name, hint in additional_type_hints.items():
                if name not in base or TypeHintComparator(hint, base[name]).match():
                    additional[name] = hint

        real = dict(super()._type_hints())

        full = ChainMap(real, additional, base)

        yield from full.items()


@lru_cache()
def _get_class_type_hints(cls: type) -> dict[str, type]:
    type_hints = dict()
    for b in cls.__mro__[::-1]:
        list_fields = {k: v for k, v in get_type_hints(b).items() if not k.startswith("_")}
        type_hints |= list_fields

    logger.debug(f"The obtained type hints for {cls!r} are: {type_hints!r}")
    return type_hints


T = TypeVar("T", bound=DeclarativeModel)
MinosModel = DeclarativeModel

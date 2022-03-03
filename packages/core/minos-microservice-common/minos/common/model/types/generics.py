from __future__ import (
    annotations,
)

from typing import (
    TYPE_CHECKING,
    Any,
    TypeVar,
    Union,
    get_args,
    get_origin,
)

if TYPE_CHECKING:
    from ..abc import (
        Model,
    )


# noinspection SpellCheckingInspection
def unpack_typevar(value: TypeVar) -> type:
    """Unpack `TypeVar` into a union of possible types.

    :param value: A type var instance.
    :return: A union of types.
    """
    return Union[value.__constraints__ or (value.__bound__ or Any,)]


class GenericTypeProjector:
    """Generic Type Projector."""

    def __init__(self, type_hints: dict[str, type], mapper: dict[TypeVar, type]):
        self.type_hints = type_hints
        self.mapper = mapper

    @classmethod
    def from_model(cls, model: Union[Model, type[Model]]) -> GenericTypeProjector:
        """Build a new instance from model.

        :param model: The model class.
        :return: A ``GenericTypeProjector`` instance.
        """
        # noinspection PyTypeChecker
        generics_ = dict(zip(model.type_hints_parameters, get_args(model)))
        # noinspection PyTypeChecker
        return cls(model.type_hints, generics_)

    def build(self) -> dict[str, type]:
        """Builder a projection of type vars values.

        :return: A dict of type hints.
        """
        return {k: self._build(v) for k, v in self.type_hints.items()}

    def _build(self, type_: type) -> type:
        if isinstance(type_, TypeVar):
            return self.mapper.get(type_, unpack_typevar(type_))

        origin = get_origin(type_)
        if origin is None:
            return type_

        # noinspection PyUnresolvedReferences
        return self._build(origin)[tuple(self._build(arg) for arg in get_args(type_))]

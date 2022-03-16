from __future__ import (
    annotations,
)

import types
from typing import (
    TYPE_CHECKING,
    Generic,
    TypeVar,
    Union,
    _GenericAlias,
    get_args,
)

from .mixins import (
    InjectableMixin,
)

if TYPE_CHECKING:
    InputType = TypeVar("InputType", bound=type)

    Output = Union[InputType, InjectableMixin]
    OutputType = type[Output]


class Injectable:
    """TODO"""

    def __init__(self, name: str):
        self._name = name

    def __call__(self, input_type: InputType) -> OutputType:
        bases = (input_type, InjectableMixin)
        if (generic := self._build_generic(input_type)) is not None:
            bases = (*bases, generic)

        output_type = types.new_class(input_type.__name__, bases, {})

        # noinspection PyProtectedMember
        output_type._set_injectable_name(self._name)

        return output_type

    @staticmethod
    def _build_generic(type_):
        generic = next(
            (base for base in getattr(type_, "__orig_bases__", tuple()) if isinstance(base, _GenericAlias)), None
        )
        if generic is None:
            return None

        generics = tuple(a for a in get_args(generic) if isinstance(a, TypeVar))
        if not len(generics):
            return None

        # noinspection PyTypeHints
        return Generic[generics]

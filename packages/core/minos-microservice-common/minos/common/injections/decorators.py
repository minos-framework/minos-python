from __future__ import (
    annotations,
)

import types
from typing import (
    TYPE_CHECKING,
    Generic,
    TypeVar,
    get_args, get_origin,
)

from .mixins import (
    InjectableMixin,
)

if TYPE_CHECKING:
    InputType = TypeVar("InputType", bound=type)

    class _Output(InputType, InjectableMixin):
        """For typing purposes only."""

    OutputType = type[_Output]


class Injectable:
    """TODO"""

    def __init__(self, name: str):
        self._name = name

    def __call__(self, input_type: InputType) -> OutputType:
        bases = (input_type, InjectableMixin)
        if (generic := self._build_generic(input_type)) is not None:
            bases = (*bases, generic)

        # noinspection PyTypeChecker
        output_type: OutputType = types.new_class(input_type.__name__, bases, {})

        # noinspection PyProtectedMember
        output_type._set_injectable_name(self._name)

        return output_type

    @staticmethod
    def _build_generic(type_):
        generic = next(
            (base for base in getattr(type_, "__orig_bases__", tuple()) if get_origin(base) is not None), None
        )
        if generic is None:
            return None

        generics = tuple(arg for arg in get_args(generic) if isinstance(arg, TypeVar))
        if not len(generics):
            return None

        # noinspection PyTypeHints
        return Generic[generics]

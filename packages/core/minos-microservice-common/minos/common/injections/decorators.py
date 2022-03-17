from __future__ import (
    annotations,
)

import types
from asyncio import (
    iscoroutinefunction,
)
from contextlib import (
    suppress,
)
from functools import (
    wraps,
)
from typing import (
    TYPE_CHECKING,
    Generic,
    TypeVar,
    Union,
    get_args,
    get_origin,
    get_type_hints,
)

from dependency_injector.containers import (
    Container,
)
from dependency_injector.wiring import (
    Provide,
    inject,
)

from ..exceptions import (
    NotProvidedException,
)
from ..model.types import (
    NoneType,
    is_type_subclass,
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
    """Injectable decorator."""

    def __init__(self, name: str):
        if not isinstance(name, str):
            raise ValueError(f"'name' argument must be a {str!r} instance. Obtained: {name}")
        self._name = name

    @property
    def name(self) -> str:
        """Get the name.

        :return: A ``str`` value.
        """
        return self._name

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


class Inject:
    """TODO"""

    def __call__(self, func):
        type_hints_ = self._build_type_hints(func)

        if iscoroutinefunction(func):

            @wraps(func)
            async def _wrapper(*args, **kwargs):
                for name in type_hints_.keys() - kwargs.keys():
                    if type_hints_[name][0] < len(args):
                        continue
                    kwargs[name] = self.resolve(type_hints_[name][1])
                return await func(*args, **kwargs)

        else:

            @wraps(func)
            def _wrapper(*args, **kwargs):
                for name in type_hints_.keys() - kwargs.keys():
                    if type_hints_[name][0] < len(args):
                        continue
                    kwargs[name] = self.resolve(type_hints_[name][1])
                return func(*args, **kwargs)

        return _wrapper

    @staticmethod
    def _build_type_hints(func) -> dict[str, tuple[int, type[V]]]:
        # TODO: Improve this function.
        type_hints_ = dict()

        hints = get_type_hints(func)

        for i, name in enumerate(func.__code__.co_varnames):
            if name in ("return", "self", "cls"):
                continue
            if name not in hints:
                continue
            type_ = hints[name]
            origin_type = get_origin(type_)
            if origin_type is Union:
                some = False
                for arg in get_args(type_):
                    if is_type_subclass(arg) and issubclass(arg, InjectableMixin):
                        some = True
                if not some:
                    continue
            if is_type_subclass(origin_type) and not issubclass(origin_type, InjectableMixin):
                continue
            if origin_type is None and not issubclass(type_, InjectableMixin):
                continue
            type_hints_[name] = (i, type_)
        return type_hints_

    @classmethod
    def resolve(cls, type_: type[V]) -> V:
        """TODO"""

        origin_type = get_origin(type_)

        if origin_type is Union:
            for arg in get_args(type_):
                if is_type_subclass(arg) and issubclass(arg, InjectableMixin):
                    with suppress(NotProvidedException):
                        return cls.resolve_by_name(arg.get_injectable_name())
                elif arg is NoneType:
                    return None

            raise NotProvidedException(f"The {type_!r} argument must be injected.")

        return cls.resolve_by_name(type_.get_injectable_name())

    @staticmethod
    @inject
    def resolve_by_name(name: str, container: Container = Provide["<container>"]):
        """TODO"""
        try:
            return container.providers[name]()
        except Exception:
            raise NotProvidedException(f"The {name} injection must be provided.")


V = TypeVar("V")

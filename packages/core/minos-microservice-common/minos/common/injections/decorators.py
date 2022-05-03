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
from inspect import (
    Parameter,
    signature,
)
from typing import (
    Any,
    Generic,
    Type,
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
    is_type_subclass,
)
from .mixins import (
    InjectableMixin,
)

InputType = TypeVar("InputType", bound=type)

OutputType = Union[InputType, Type[InjectableMixin]]


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
        output_type.__module__ = input_type.__module__

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
    """Inject class."""

    def __call__(self, func):
        type_hints_ = self._build_type_hints(func)

        if iscoroutinefunction(func):

            @wraps(func)
            async def _wrapper(*args, **kwargs):
                kwargs = self._inject_on_kwargs(type_hints_, args, kwargs)
                return await func(*args, **kwargs)

        else:

            @wraps(func)
            def _wrapper(*args, **kwargs):
                kwargs = self._inject_on_kwargs(type_hints_, args, kwargs)
                return func(*args, **kwargs)

        return _wrapper

    def _inject_on_kwargs(
        self, type_hints_: dict[str, tuple[int, type[V], bool]], args: tuple[Any], kwargs: dict[str, Any]
    ) -> dict[str, Any]:
        for name in type_hints_.keys() - kwargs.keys():
            if type_hints_[name][0] < len(args):
                continue
            try:
                kwargs[name] = self.resolve(type_hints_[name][1])
            except NotProvidedException as exc:
                if not type_hints_[name][2]:
                    raise exc
        return kwargs

    def _build_type_hints(self, func) -> dict[str, tuple[int, type[V], bool]]:
        type_hints_ = dict()

        hints = get_type_hints(func)

        for i, (name, field) in enumerate(signature(func).parameters.items()):
            if name in ("return", "self", "cls") or name not in hints:
                continue
            type_ = hints[name]
            if not self._is_injectable(type_):
                continue
            type_hints_[name] = (i, type_, field.default is not Parameter.empty)
        return type_hints_

    def _is_injectable(self, type_: type) -> bool:
        origin_type = get_origin(type_)
        if origin_type is Union:
            if any(self._is_injectable(arg) for arg in get_args(type_)):
                return True
        elif origin_type is None:
            if is_type_subclass(type_):
                if issubclass(type_, InjectableMixin):
                    return True
        elif is_type_subclass(origin_type):
            if issubclass(origin_type, InjectableMixin):
                return True
        return False

    @classmethod
    def resolve(cls, type_: type[V]) -> V:
        """Resolve a dependency by type.

        :param type_: The type of the dependency.
        :return: The dependency value.
        """

        origin_type = get_origin(type_)

        if origin_type is Union:
            for arg in get_args(type_):
                with suppress(NotProvidedException):
                    return cls.resolve(arg)
        elif origin_type is None:
            if issubclass(type_, InjectableMixin):
                return cls.resolve_by_name(type_.get_injectable_name())
        elif is_type_subclass(origin_type):
            if issubclass(origin_type, InjectableMixin):
                return cls.resolve_by_name(origin_type.get_injectable_name())

        raise NotProvidedException(f"The given type injection could not be resolved: {type_}")

    @staticmethod
    @inject
    def resolve_by_name(name: str, container: Container = Provide["<container>"]) -> Any:
        """Resolve a dependency by name.

        :param name: The name of the dependency.
        :param container: The container that contains the dependency.
        :return: The dependency value.
        """
        try:
            return container.providers[name]()
        except Exception:
            raise NotProvidedException(f"The {name} injection must be provided.")


V = TypeVar("V")

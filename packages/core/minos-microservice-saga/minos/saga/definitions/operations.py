"""Operation module."""

from __future__ import (
    annotations,
)

from collections.abc import (
    Callable,
    Iterable,
)
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Optional,
    TypeVar,
    Union,
)

if TYPE_CHECKING:
    from .steps import SagaStepDecoratorMeta

from minos.common import (
    classname,
    import_module,
)

from ..context import (
    SagaContext,
)

T = TypeVar("T", bound=Callable)


class SagaOperationDecorator(Generic[T]):
    """Saga Operation Decorator class."""

    def __init__(self, attr_name: str = None, step_meta: SagaStepDecoratorMeta = None, *args, **kwargs):
        if attr_name is None:
            raise ValueError(f"The 'attr_name' must not be {None!r}.")
        if step_meta is None:
            raise ValueError(f"The 'step_meta' must not be {None!r}.")

        self._step_meta = step_meta
        self._attr_name = attr_name

        self._args = args
        self._kwargs = kwargs

    def __call__(self, func: T) -> T:
        operation = SagaOperation(func, *self._args, **self._kwargs)
        setattr(self._step_meta, self._attr_name, operation)
        return func


class SagaOperation(Generic[T]):
    """Saga Step Operation class."""

    def __init__(self, callback: T, parameters: Optional[Union[dict, SagaContext]] = None, **kwargs):
        if parameters is None and len(kwargs):
            parameters = kwargs
        if parameters is not None and not isinstance(parameters, SagaContext):
            parameters = SagaContext(**parameters)

        self.callback = callback
        self.parameters = parameters

    @property
    def raw(self) -> dict[str, Any]:
        """Generate a rew representation of the instance.

        :return: A ``dict`` instance.
        """
        # noinspection PyTypeChecker
        raw = {"callback": classname(self.callback)}

        if self.parameterized:
            raw["parameters"] = self.parameters.avro_str

        return raw

    @property
    def parameterized(self) -> bool:
        """parameterized getter.

        :return: ``True`` if parameters are provided or ``False`` otherwise.
        """
        return self.parameters is not None

    @classmethod
    def from_raw(cls, raw: Optional[Union[dict[str, Any], SagaOperation[T]]], **kwargs) -> Optional[SagaOperation[T]]:
        """Build a new instance from a raw representation.

        :param raw: A raw representation.
        :param kwargs: Additional named arguments.
        :return: A ``SagaStepOperation`` instance if the ``raw`` argument is not ``None``, ``None`` otherwise.
        """
        if raw is None:
            return None

        if isinstance(raw, cls):
            return raw

        current = raw | kwargs
        if isinstance(current["callback"], str):
            current["callback"] = import_module(current["callback"])

        if "parameters" in current:
            current["parameters"] = SagaContext.from_avro_str(current["parameters"])
        return cls(**current)

    def __hash__(self):
        return hash(tuple(self))

    def __eq__(self, other: SagaOperation) -> bool:
        return type(self) == type(other) and tuple(self) == tuple(other)

    def __repr__(self) -> str:
        return f"{type(self).__name__}{tuple(self)}"

    def __iter__(self) -> Iterable:
        yield from (
            self.callback,
            self.parameters,
        )

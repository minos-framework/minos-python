"""Base Step Definitions module."""

from __future__ import (
    annotations,
)

import warnings
from abc import (
    ABC,
    abstractmethod,
)
from collections.abc import (
    Callable,
    Iterable,
)
from typing import (
    TYPE_CHECKING,
    Any,
    Optional,
    Protocol,
    Union,
    runtime_checkable,
)

from minos.common import (
    import_module,
)

from ...exceptions import (
    SagaNotDefinedException,
)

if TYPE_CHECKING:
    from ..saga import (
        Saga,
    )
    from .conditional import (
        ConditionalSagaStep,
    )
    from .local import (
        LocalSagaStep,
    )
    from .remote import (
        RemoteSagaStep,
    )


@runtime_checkable
class SagaStepDecoratorWrapper(Protocol):
    """Saga Step Decorator Wrapper class."""

    meta: SagaStepDecoratorMeta


class SagaStepDecoratorMeta:
    """Saga Step Decorator Meta class."""

    _inner: Any
    _definition: SagaStep

    def __init__(self, inner: Any, definition: SagaStep):
        self._inner = inner
        self._definition = definition

    @property
    @abstractmethod
    def definition(self) -> SagaStep:
        """Get the step definition.

        :return: A ``SagaStep`` instance.
        """


class SagaStep(ABC):
    """Saga step class."""

    def __init__(self, saga: Optional[Saga] = None, order: Optional[int] = None, **kwargs):
        self.saga = saga
        self.order = order

    @classmethod
    def from_raw(cls, raw: Union[dict[str, Any], SagaStep], **kwargs) -> SagaStep:
        """Build a new instance from raw.

        :param raw: A raw representation.
        :param kwargs: Additional named arguments.
        :return: A ``SagaStep`` instance.
        """
        if isinstance(raw, cls):
            return raw

        raw = raw.copy()

        if "cls" in raw:
            # noinspection PyTypeChecker
            step_cls: type = import_module(raw.pop("cls"))
        else:
            step_cls = cls

        if not issubclass(step_cls, cls):
            raise TypeError(f"Given class is not a subclass of {cls}. Obtained: {step_cls}")

        return step_cls._from_raw(raw | kwargs)

    @classmethod
    @abstractmethod
    def _from_raw(cls, raw: dict[str, Any]) -> SagaStep:
        raise NotImplementedError

    @abstractmethod
    def __call__(self, func: Callable) -> SagaStepDecoratorWrapper:
        """Decorate the given function.

        :param func: The function to be decorated.
        :return: The decorated function.
        """

    def conditional_step(self, *args, **kwargs) -> ConditionalSagaStep:
        """Create a new conditional step in the ``Saga``.

        :param args: Additional positional parameters.
        :param kwargs: Additional named parameters.
        :return: A new ``SagaStep`` instance.
        """
        self.validate()
        if self.saga is None:
            raise SagaNotDefinedException()
        return self.saga.conditional_step(*args, **kwargs)

    def local_step(self, *args, **kwargs) -> LocalSagaStep:
        """Create a new local step in the ``Saga``.

        :param args: Additional positional parameters.
        :param kwargs: Additional named parameters.
        :return: A new ``SagaStep`` instance.
        """
        self.validate()
        if self.saga is None:
            raise SagaNotDefinedException()
        return self.saga.local_step(*args, **kwargs)

    def step(self, *args, **kwargs) -> SagaStep:
        """Create a new step in the ``Saga``.

        :param args: Additional positional parameters.
        :param kwargs: Additional named parameters.
        :return: A new ``SagaStep`` instance.
        """
        warnings.warn("step() method is deprecated by remote_step() and will be removed soon.", DeprecationWarning)
        return self.remote_step(*args, **kwargs)

    def remote_step(self, *args, **kwargs) -> RemoteSagaStep:
        """Create a new remote step in the ``Saga``.

        :param args: Additional positional parameters.
        :param kwargs: Additional named parameters.
        :return: A new ``SagaStep`` instance.
        """
        self.validate()
        if self.saga is None:
            raise SagaNotDefinedException()
        return self.saga.remote_step(*args, **kwargs)

    def commit(self, *args, **kwargs) -> Saga:
        """Commit the current ``SagaStep`` on the ``Saga``.

        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: A ``Saga`` instance.
        """
        self.validate()
        if self.saga is None:
            raise SagaNotDefinedException()
        return self.saga.commit(*args, **kwargs)

    @abstractmethod
    def validate(self) -> None:
        """Check if the step is valid.

        :return: This method does not return anything, but raises an exception if the step is not valid.
        """

    @property
    @abstractmethod
    def raw(self) -> dict[str, Any]:
        """Generate a raw representation of the instance.

        :return: A ``dict`` instance.
        """

    def __hash__(self):
        return hash(tuple(self))

    def __eq__(self, other: SagaStep) -> bool:
        return type(self) == type(other) and tuple(self) == tuple(other)

    def __repr__(self) -> str:
        return f"{type(self).__name__}{tuple(self)}"

    @abstractmethod
    def __iter__(self) -> Iterable:
        """Iterate over the ``SagaStep`` attributes.

        :return: An iterable of attributes.
        """

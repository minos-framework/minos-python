"""Saga definitions module."""

from __future__ import (
    annotations,
)

import warnings
from collections.abc import (
    Iterable,
)
from inspect import (
    getmembers,
)
from operator import (
    attrgetter,
)
from typing import (
    Any,
    Optional,
    Protocol,
    TypeVar,
    Union,
    runtime_checkable,
)

from cached_property import (
    cached_property,
)

from ..exceptions import (
    AlreadyCommittedException,
    AlreadyOnSagaException,
    EmptySagaException,
    OrderPrecedenceException,
    SagaNotCommittedException,
)
from .operations import (
    SagaOperation,
)
from .steps import (
    ConditionalSagaStep,
    LocalSagaStep,
    RemoteSagaStep,
    SagaStep,
    SagaStepDecoratorWrapper,
)
from .types import (
    LocalCallback,
    RequestCallBack,
)


@runtime_checkable
class SagaDecoratorWrapper(Protocol):
    """Saga Decorator Wrapper class."""

    meta: SagaDecoratorMeta


class SagaDecoratorMeta:
    """Saga Decorator Meta class."""

    _inner: type
    _definition: Saga

    def __init__(self, func: type, saga: Saga):
        self._inner = func
        self._definition = saga

    @cached_property
    def definition(self) -> Saga:
        """Get the saga definition.

        :return: A ``Saga`` instance.
        """
        steps = getmembers(self._inner, predicate=lambda x: isinstance(x, SagaStepDecoratorWrapper))
        steps = list(map(lambda member: member[1].meta.definition, steps))
        for step in steps:
            if step.order is None:
                raise OrderPrecedenceException(f"The {step!r} step does not have 'order' value.")
        steps.sort(key=attrgetter("order"))

        for step in steps:
            self._definition.add_step(step)
        self._definition.commit()

        return self._definition


TP = TypeVar("TP", bound=type)


class Saga:
    """Saga class.

    The purpose of this class is to define a sequence of operations among microservices.
    """

    # noinspection PyUnusedLocal
    def __init__(
        self, *args, steps: list[SagaStep] = None, committed: Optional[bool] = None, commit: None = None, **kwargs
    ):
        self.steps = list()
        self.committed = False

        if steps is None:
            steps = list()
        for step in steps:
            self.add_step(step)

        if committed is None:
            committed = len(steps)
        self.committed = committed

        if commit is not None:
            warnings.warn(f"Commit callback is being deprecated. Use {self.local_step!r} instead", DeprecationWarning)
            self.local_step(commit)
            self.committed = True

    def __call__(self, type_: TP) -> Union[TP, SagaDecoratorWrapper]:
        """Decorate the given type.

        :param type_: The type to be decorated.
        :return: The decorated type.
        """
        type_.meta = SagaDecoratorMeta(type_, self)
        return type_

    @classmethod
    def from_raw(cls, raw: Union[dict[str, Any], Saga], **kwargs) -> Saga:
        """Build a new ``Saga`` instance from raw.

        :param raw: The raw representation of the saga.
        :param kwargs: Additional named arguments.
        :return: A new ``Saga`` instance.
        """
        if isinstance(raw, cls):
            return raw

        current = raw | kwargs

        steps = [SagaStep.from_raw(step) for step in current.pop("steps")]

        instance = cls(steps=steps, **current)

        return instance

    def conditional_step(self, step: Optional[ConditionalSagaStep] = None) -> ConditionalSagaStep:
        """Add a new conditional step.

        :param step: The step to be added. If `None` is provided then a new one will be created.
        :return: A ``SagaStep`` instance.
        """
        return self.add_step(step, ConditionalSagaStep)

    def local_step(
        self, step: Optional[Union[LocalCallback, SagaOperation[LocalCallback], LocalSagaStep]] = None, **kwargs
    ) -> LocalSagaStep:
        """Add a new local step.

        :param step: The step to be added. If `None` is provided then a new one will be created.
        :param kwargs: Additional named parameters.
        :return: A ``SagaStep`` instance.
        """
        if step is not None and not isinstance(step, SagaStep) and not isinstance(step, SagaOperation):
            step = SagaOperation(step, **kwargs)
        return self.add_step(step, LocalSagaStep)

    def step(
        self, step: Optional[Union[RequestCallBack, SagaOperation[RequestCallBack], RemoteSagaStep]] = None, **kwargs
    ) -> RemoteSagaStep:
        """Add a new remote step.

        :param step: The step to be added. If `None` is provided then a new one will be created.
        :param kwargs: Additional named parameters.
        :return: A ``SagaStep`` instance.
        """
        warnings.warn("step() method is deprecated by remote_step() and will be removed soon.", DeprecationWarning)
        return self.remote_step(step, **kwargs)

    def remote_step(
        self, step: Optional[Union[RequestCallBack, SagaOperation[RequestCallBack], RemoteSagaStep]] = None, **kwargs
    ) -> RemoteSagaStep:
        """Add a new remote step.

        :param step: The step to be added. If `None` is provided then a new one will be created.
        :param kwargs: Additional named parameters.
        :return: A ``SagaStep`` instance.
        """
        if step is not None and not isinstance(step, SagaStep) and not isinstance(step, SagaOperation):
            step = SagaOperation(step, **kwargs)
        return self.add_step(step, RemoteSagaStep)

    def add_step(self, step: Optional[Union[SagaOperation, T]], step_cls: type[T] = SagaStep) -> T:
        """Add a new step.

        :param step: The step to be added.
        :param step_cls: The step class (for validation purposes).
        :return: The added step.
        """
        if self.committed:
            raise AlreadyCommittedException("It is not possible to add more steps to an already committed saga.")

        if step is None:
            step = step_cls(saga=self)
        elif isinstance(step, SagaStep):
            if not isinstance(step, step_cls):
                raise TypeError(f"The given step does not match the requested type: {step_cls} vs {type(step)}")
            if step.saga is not None:
                raise AlreadyOnSagaException()
            step.saga = self

        else:
            step = step_cls(step, saga=self)

        if step.order is None:
            if self.steps:
                step.order = self.steps[-1].order + 1
            else:
                step.order = 1

        if self.steps and step.order <= self.steps[-1].order:
            raise OrderPrecedenceException(
                f"Unsatisfied precedence constraints. Previous: {self.steps[-1].order} Current: {step.order} "
            )

        self.steps.append(step)
        return step

    @property
    def raw(self) -> dict[str, Any]:
        """Generate a raw representation of the instance.

        :return: A ``dict`` instance.
        """
        ans = {
            "steps": [step.raw for step in self.steps],
            "committed": self.committed,
        }
        return ans

    def __eq__(self, other: SagaStep) -> bool:
        return type(self) == type(other) and tuple(self) == tuple(other)

    def __iter__(self) -> Iterable:
        yield from (
            self.steps,
            self.committed,
        )

    def commit(self, callback: None = None, **kwargs) -> Saga:
        """Commit the instance to be ready for execution.

        :param callback: Deprecated argument.
        :param kwargs: Additional named arguments.
        :return: A ``Saga`` instance.
        """
        if self.committed:
            raise AlreadyCommittedException("It is not possible to commit a saga multiple times.")

        if callback is not None:
            warnings.warn(f"Commit callback is being deprecated. Use {self.local_step!r} instead", DeprecationWarning)
            self.local_step(callback, **kwargs)

        self.committed = True
        return self

    def validate(self) -> None:
        """Check if the saga is valid.

        :return: This method does not return anything, but raises an exception if the saga is not valid.
        """

        if not len(self.steps):
            raise EmptySagaException()

        for step in self.steps:
            step.validate()

        if not self.committed:
            raise SagaNotCommittedException()


T = TypeVar("T")

from __future__ import (
    annotations,
)

import warnings
from inspect import (
    getmembers,
)
from operator import (
    attrgetter,
    itemgetter,
)
from typing import (
    Any,
    Iterable,
    Optional,
    Protocol,
    Type,
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
    SagaStepWrapper,
)
from .types import (
    LocalCallback,
    RequestCallBack,
)

SagaType = TypeVar("SagaType", bound=type)


@runtime_checkable
class SagaWrapper(Protocol):
    """TODO"""

    meta: SagaMeta


class SagaMeta:
    """TODO"""

    func: SagaType
    _saga: Saga

    def __init__(self, func: SagaType, saga: Saga):
        self.func = func
        self._saga = saga

    @cached_property
    def saga(self):
        callables: list[SagaStepWrapper] = sorted(
            set(map(itemgetter(1), getmembers(self.func, predicate=lambda x: isinstance(x, SagaStepWrapper)))),
            key=attrgetter("meta.saga_step.priority"),
        )
        for c in callables:
            self._saga.steps.append(c.meta.saga_step)
        self._saga.commit()

        return self._saga


class Saga:
    """Saga class.

    The purpose of this class is to define a sequence of operations among microservices.
    """

    # noinspection PyUnusedLocal
    def __init__(
        self,
        decorated: Optional[type] = None,
        *args,
        steps: list[SagaStep] = None,
        committed: Optional[bool] = None,
        commit: None = None,
        **kwargs,
    ):
        if decorated is not None:
            steps = sorted(
                map(itemgetter(1), getmembers(decorated, predicate=lambda x: isinstance(x, SagaStep))),
                key=lambda step: step.priority,
            )

        if steps is None:
            steps = list()

        if committed is None:
            committed = len(steps)

        self.steps = steps
        self.committed = committed

        if commit is not None:
            warnings.warn(f"Commit callback is being deprecated. Use {self.local_step!r} instead", DeprecationWarning)
            self.local_step(commit)
            self.committed = True

    def __call__(self, type_: Union[type, SagaWrapper]) -> SagaWrapper:
        if not isinstance(type_, SagaWrapper):
            type_.meta = SagaMeta(type_, self)

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
        return self._add_step(ConditionalSagaStep, step)

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
        return self._add_step(LocalSagaStep, step)

    def step(
        self, step: Optional[Union[RequestCallBack, SagaOperation[RequestCallBack], RemoteSagaStep]] = None, **kwargs
    ) -> RemoteSagaStep:
        """Add a new remote step step.

        :param step: The step to be added. If `None` is provided then a new one will be created.
        :param kwargs: Additional named parameters.
        :return: A ``SagaStep`` instance.
        """
        warnings.warn("step() method is deprecated by remote_step() and will be removed soon.", DeprecationWarning)
        return self.remote_step(step, **kwargs)

    def remote_step(
        self, step: Optional[Union[RequestCallBack, SagaOperation[RequestCallBack], RemoteSagaStep]] = None, **kwargs
    ) -> RemoteSagaStep:
        """Add a new remote step step.

        :param step: The step to be added. If `None` is provided then a new one will be created.
        :param kwargs: Additional named parameters.
        :return: A ``SagaStep`` instance.
        """
        if step is not None and not isinstance(step, SagaStep) and not isinstance(step, SagaOperation):
            step = SagaOperation(step, **kwargs)
        return self._add_step(RemoteSagaStep, step)

    def _add_step(self, step_cls: Type[T], step: Optional[Union[SagaOperation, T]]) -> T:
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

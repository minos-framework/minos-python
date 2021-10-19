from __future__ import (
    annotations,
)

import warnings
from typing import (
    Any,
    Iterable,
    Optional,
    Type,
    TypeVar,
    Union,
)

from ..context import (
    SagaContext,
)
from ..exceptions import (
    AlreadyCommittedException,
    AlreadyOnSagaException,
    EmptySagaException,
    SagaNotCommittedException,
)
from .operations import (
    SagaOperation,
    identity_fn,
)
from .steps import (
    ConditionalSagaStep,
    LocalSagaStep,
    RemoteSagaStep,
    SagaStep,
)
from .types import (
    LocalCallback,
    RequestCallBack,
)


class Saga:
    """Saga class.

    The purpose of this class is to define a sequence of operations among microservices.
    """

    # noinspection PyUnusedLocal
    def __init__(
        self,
        *args,
        steps: list[SagaStep] = None,
        commit: Optional[Union[LocalCallback, SagaOperation[LocalCallback]]] = None,
        **kwargs,
    ):
        if steps is None:
            steps = list()
        if commit is not None and not isinstance(commit, SagaOperation):
            commit = SagaOperation(commit)

        self.steps = steps
        self.commit_operation = commit

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

        commit = current.pop("commit", None)
        if commit is not None:
            commit = SagaOperation.from_raw(commit)

        steps = [SagaStep.from_raw(step) for step in current.pop("steps")]

        instance = cls(steps=steps, commit=commit, **current)

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
            "commit": None if self.commit_operation is None else self.commit_operation.raw,
        }
        return ans

    def __eq__(self, other: SagaStep) -> bool:
        return type(self) == type(other) and tuple(self) == tuple(other)

    def __iter__(self) -> Iterable:
        yield from (
            self.steps,
            self.commit_operation,
        )

    # noinspection PyUnusedLocal
    def commit(
        self, callback: Optional[LocalCallback] = None, parameters: Optional[SagaContext] = None, **kwargs
    ) -> Saga:
        """Commit the instance to be ready for execution.

        :param callback: Optional function to be called at the end of execution.
        :param parameters: A mapping of named parameters to be passed to the callback.
        :param kwargs: A set of named arguments to be passed to the callback. ``parameters`` has priority if it is not
            ``None``.
        :return: A ``Saga`` instance.
        """
        if self.committed:
            raise AlreadyCommittedException("It is not possible to commit a saga multiple times.")

        if callback is None:
            callback = identity_fn

        self.commit_operation = SagaOperation(callback, parameters=parameters, **kwargs)
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

    @property
    def committed(self) -> bool:
        """Check if the instance is already committed.

        :return: A boolean value.
        """
        return self.commit_operation is not None


T = TypeVar("T")

from __future__ import (
    annotations,
)

from typing import (
    Any,
    Iterable,
    Optional,
    Union,
)

from ..context import (
    SagaContext,
)
from ..exceptions import (
    AlreadyCommittedException,
    AlreadyOnSagaException,
)
from .operations import (
    SagaOperation,
    identity_fn,
)
from .step import (
    SagaStep,
)
from .types import (
    CommitCallback,
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
        commit: Optional[Union[CommitCallback, SagaOperation]] = None,
        **kwargs
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

    def step(self, step: Optional[Union[RequestCallBack, SagaOperation, SagaStep]] = None, **kwargs) -> SagaStep:
        """Add a new step in the ``Saga``.

        :return: A ``SagaStep`` instance.
        """
        if self.committed:
            raise AlreadyCommittedException("It is not possible to add more steps to an already committed saga.")

        if step is None:
            step = SagaStep(saga=self)
        elif isinstance(step, SagaStep):
            if step.saga is not None:
                raise AlreadyOnSagaException()
            step.saga = self
        elif isinstance(step, SagaOperation):
            step = SagaStep(step, saga=self)
        else:
            step = SagaStep(on_execute=SagaOperation(step, **kwargs), saga=self)

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
        self, callback: Optional[CommitCallback] = None, parameters: Optional[SagaContext] = None, **kwargs
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

    @property
    def committed(self) -> bool:
        """Check if the instance is already committed.

        :return: A boolean value.
        """
        return self.commit_operation is not None

"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

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
    MinosAlreadyOnSagaException,
    MinosSagaAlreadyCommittedException,
)
from .operations import (
    SagaOperation,
)
from .step import (
    SagaStep,
    identity_fn,
)
from .types import (
    CommitCallback,
)


class Saga(object):
    """Saga class.

    The purpose of this class is to define a sequence of operations among microservices.
    """

    def __init__(
        self, name: str, steps: list[SagaStep] = None, commit_operation: Optional[SagaOperation] = None,
    ):
        if steps is None:
            steps = list()

        self.name = name
        self.steps = steps
        self.commit_operation = commit_operation

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

        commit_operation = current.pop("commit", None)
        if commit_operation is not None:
            commit_operation = SagaOperation.from_raw(commit_operation)

        steps = [SagaStep.from_raw(step) for step in current.pop("steps")]

        instance = cls(steps=steps, commit_operation=commit_operation, **current)

        return instance

    def step(self, step: Optional[SagaStep] = None) -> SagaStep:
        """Add a new step in the ``Saga``.

        :return: A ``SagaStep`` instance.
        """
        if self.committed:
            raise MinosSagaAlreadyCommittedException(
                "It is not possible to add more steps to an already committed saga."
            )

        if step is None:
            step = SagaStep(self)
        else:
            if step.saga is not None:
                raise MinosAlreadyOnSagaException()
            step.saga = self

        self.steps.append(step)
        return step

    @property
    def raw(self) -> dict[str, Any]:
        """Generate a raw representation of the instance.

        :return: A ``dict`` instance.
        """
        ans = {
            "name": self.name,
            "steps": [step.raw for step in self.steps],
            "commit": None if self.commit_operation is None else self.commit_operation.raw,
        }
        return ans

    def __eq__(self, other: SagaStep) -> bool:
        return type(self) == type(other) and tuple(self) == tuple(other)

    def __iter__(self) -> Iterable:
        yield from (
            self.name,
            self.steps,
            self.commit_operation,
        )

    # noinspection PyUnusedLocal
    def commit(self, callback: Optional[CommitCallback] = None, parameters: Optional[SagaContext] = None) -> Saga:
        """Commit the instance to be ready for execution.

        :param callback: Optional function to be called at the end of execution.
        :param parameters: A mapping of named parameters to be passed to the callback.
        :return: A ``Saga`` instance.
        """
        if self.committed:
            raise MinosSagaAlreadyCommittedException("It is not possible to commit a saga multiple times.")

        if callback is None:
            callback = identity_fn

        self.commit_operation = SagaOperation(callback, parameters=parameters)
        return self

    @property
    def committed(self) -> bool:
        """Check if the instance is already committed.

        :return: A boolean value.
        """
        return self.commit_operation is not None

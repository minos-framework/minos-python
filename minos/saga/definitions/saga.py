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

from ..exceptions import (
    MinosAlreadyOnSagaException,
)
from .step import (
    SagaStep,
)


class Saga(object):
    """Saga class.

    The purpose of this class is to define a sequence of operations among microservices.
    """

    def __init__(self, name: str, steps: list[SagaStep] = None):
        if steps is None:
            steps = list()

        self.name = name
        self.steps = steps

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
        steps = (SagaStep.from_raw(step) for step in current.pop("steps"))

        instance = cls(**current)
        for step in steps:
            instance.step(step)
        return instance

    def step(self, step: Optional[SagaStep] = None) -> SagaStep:
        """Add a new step in the ``Saga``.

        :return: A ``SagaStep`` instance.
        """
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
        return {
            "name": self.name,
            "steps": [step.raw for step in self.steps],
        }

    def __eq__(self, other: SagaStep) -> bool:
        return type(self) == type(other) and tuple(self) == tuple(other)

    def __iter__(self) -> Iterable:
        yield from (
            self.name,
            self.steps,
        )

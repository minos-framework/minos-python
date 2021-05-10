"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

from __future__ import (
    annotations,
)

from typing import (
    TYPE_CHECKING,
    Any,
    Iterable,
    Optional,
    Union,
)

from ..exceptions import (
    MinosAlreadyOnSagaException,
)
from .abc import (
    MinosBaseSagaBuilder,
)
from .step import (
    SagaStep,
)

if TYPE_CHECKING:
    from ..executions import (
        SagaExecution,
    )


class Saga(MinosBaseSagaBuilder):
    """TODO"""

    def __init__(self, name: str, steps: list[SagaStep] = None):
        if steps is None:
            steps = list()

        self.name = name
        self.steps = steps

    @classmethod
    def from_raw(cls, raw: Union[dict[str, Any], Saga], **kwargs) -> Saga:
        """TODO

        :param raw: TODO
        :param kwargs: TODO
        :return: TODO
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
        """TODO

        :return: TODO
        """
        if step is None:
            step = SagaStep(self)
        else:
            if step.saga is not None:
                raise MinosAlreadyOnSagaException()
            step.saga = self

        self.steps.append(step)
        return step

    def build_execution(self, *args, **kwargs) -> SagaExecution:
        """TODO

        :return: TODO
        """
        from ..executions import (
            SagaExecution,
        )

        return SagaExecution.from_saga(self, *args, **kwargs)

    @property
    def raw(self) -> dict[str, Any]:
        """TODO

        :return: TODO
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

from __future__ import (
    annotations,
)

from typing import (
    TYPE_CHECKING,
    Any,
    Awaitable,
    Callable,
    Iterable,
    Optional,
    Union,
)

from .abc import (
    SagaStep,
)

if TYPE_CHECKING:
    from ...context import (
        SagaContext,
    )
    from ..saga import (
        Saga,
    )


class ConditionalSagaStep(SagaStep):
    """TODO"""

    def __init__(
        self, if_then: Optional[set[IfThenCondition]] = None, else_then: Optional[ElseThenCondition] = None, **kwargs
    ):
        super().__init__(**kwargs)

        if if_then is None:
            if_then = set()

        self.if_then_conditions = if_then
        self.else_then_condition = else_then

    @classmethod
    def _from_raw(cls, raw: dict[str, Any], **kwargs) -> ConditionalSagaStep:
        pass

    def if_then(
        self, condition: Callable[[SagaContext], Union[bool, Awaitable[bool]]], saga: Saga
    ) -> ConditionalSagaStep:
        """TODO"""

    def else_then(self, saga: Saga) -> ConditionalSagaStep:
        """TODO"""

    def validate(self) -> None:
        """TODO"""

    @property
    def raw(self) -> dict[str, Any]:
        """TODO"""

    def __iter__(self) -> Iterable:
        pass


class IfThenCondition:
    """TODO"""

    def __init__(self, condition, saga: Saga):
        self.condition = condition
        self.saga = saga

    def from_raw(self, raw) -> IfThenCondition:
        pass

    @property
    def raw(self) -> dict[str, Any]:
        pass


class ElseThenCondition:
    """TODO"""

    def __init__(self, saga: Saga):
        self.saga = saga

    def from_raw(self, raw) -> ElseThenCondition:
        pass

    @property
    def raw(self) -> dict[str, Any]:
        pass

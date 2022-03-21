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

from minos.common import (
    classname,
)

from ...exceptions import (
    EmptySagaStepException,
    MultipleElseThenException,
)
from ..operations import (
    SagaOperation,
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
    """Conditional Saga Step class."""

    if_then_alternatives: list[IfThenAlternative]
    else_then_alternative: ElseThenAlternative

    def __init__(
        self,
        if_then: Optional[Union[IfThenAlternative, Iterable[IfThenAlternative]]] = None,
        else_then: Optional[ElseThenAlternative] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        if isinstance(if_then, IfThenAlternative):
            if_then = [if_then]

        if if_then is None:
            if_then = list()

        self.if_then_alternatives = list(if_then)
        self.else_then_alternative = else_then

    @classmethod
    def _from_raw(cls, raw: dict[str, Any], **kwargs) -> ConditionalSagaStep:
        current = raw | kwargs

        if_then = current.get("if_then", None)
        if if_then is not None:
            current["if_then"] = [IfThenAlternative.from_raw(alternative) for alternative in if_then]

        else_then = current.get("else_then", None)
        if else_then is not None:
            current["else_then"] = ElseThenAlternative.from_raw(else_then)

        return cls(**current)

    def if_then(
        self, condition: Callable[[SagaContext], Union[bool, Awaitable[bool]]], saga: Saga
    ) -> ConditionalSagaStep:
        """Add a new ``IfThenAlternative`` based on a condition and a saga.

        :param condition: The condition that must be satisfied to execute the alternative.
        :param saga: The saga to be executed if the condition is satisfied.
        :return: This method returns the same instance that is called.
        """

        alternative = IfThenAlternative(condition, saga)
        self.if_then_alternatives.append(alternative)
        return self

    def else_then(self, saga: Saga) -> ConditionalSagaStep:
        """Set the ``ElseThenAlternative`` with the given saga.

        :param saga: The saga to be executed if not any condition is met.
        :return: This method returns the same instance that is called.
        """

        if self.else_then_alternative is not None:
            raise MultipleElseThenException()

        alternative = ElseThenAlternative(saga)
        self.else_then_alternative = alternative
        return self

    def validate(self) -> None:
        """Check if the step is valid.

        :return: This method does not return anything, but raises an exception if the step is not valid.
        """

        if not len(self.if_then_alternatives) and self.else_then_alternative is None:
            raise EmptySagaStepException()

        for alternative in self.if_then_alternatives:
            alternative.validate()

        if self.else_then_alternative is not None:
            self.else_then_alternative.validate()

    @property
    def raw(self) -> dict[str, Any]:
        """Get the raw representation of the step.

        :return: A ``dict`` instance.
        """

        return {
            "cls": classname(type(self)),
            "if_then": [alternative.raw for alternative in self.if_then_alternatives],
            "else_then": None if self.else_then_alternative is None else self.else_then_alternative.raw,
        }

    def __iter__(self) -> Iterable:
        yield from (
            self.if_then_alternatives,
            self.else_then_alternative,
        )


class IfThenAlternative:
    """If Then Alternative class."""

    def __init__(
        self, condition: Union[SagaOperation, Callable[[SagaContext], Union[bool, Awaitable[bool]]]], saga: Saga
    ):
        if not isinstance(condition, SagaOperation):
            condition = SagaOperation(condition)

        self.condition = condition
        self.saga = saga

    @classmethod
    def from_raw(cls, raw: Union[dict[str, Any], IfThenAlternative], **kwargs) -> IfThenAlternative:
        """Build a new instance from a raw representation.

        :param raw: The raw representation.
        :param kwargs: Additional named arguments.
        :return: A new ``IfThenAlternative`` instance.
        """

        from ..saga import (
            Saga,
        )

        if isinstance(raw, cls):
            return raw
        current = raw | kwargs

        current["condition"] = SagaOperation.from_raw(current["condition"])
        current["saga"] = Saga.from_raw(current["saga"])

        return cls(**current)

    def validate(self) -> None:
        """Check if the alternative is valid.

        :return: This method does not return anything, but raises an exception if the alternative is not valid.
        """

        return self.saga.validate()

    @property
    def raw(self) -> dict[str, Any]:
        """Get the raw representation of the alternative.

        :return: A ``dict`` value.
        """

        return {
            "condition": self.condition.raw,
            "saga": self.saga.raw,
        }

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, type(self)) and tuple(self) == tuple(other)

    def __iter__(self):
        yield from (
            self.condition,
            self.saga,
        )


class ElseThenAlternative:
    """Else Then Alternative class."""

    def __init__(self, saga: Saga):
        self.saga = saga

    @classmethod
    def from_raw(cls, raw: Union[dict[str, Any], ElseThenAlternative], **kwargs) -> ElseThenAlternative:
        """Build a new instance from a raw representation.

        :param raw: The raw representation.
        :param kwargs: Additional named arguments.
        :return: A new ``ElseThenAlternative`` instance.
        """

        from ..saga import (
            Saga,
        )

        if isinstance(raw, cls):
            return raw
        current = raw | kwargs

        current["saga"] = Saga.from_raw(current["saga"])

        return cls(**current)

    def validate(self) -> None:
        """Check if the alternative is valid.

        :return: This method does not return anything, but raises an exception if the alternative is not valid.
        """

        return self.saga.validate()

    @property
    def raw(self) -> dict[str, Any]:
        """Get the raw representation of the alternative.

        :return: A ``dict`` value.
        """

        return {
            "saga": self.saga.raw,
        }

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, type(self)) and tuple(self) == tuple(other)

    def __iter__(self):
        yield from (self.saga,)

from __future__ import (
    annotations,
)

from collections.abc import (
    Callable,
    Iterable,
)
from inspect import (
    getmembers,
)
from operator import (
    attrgetter,
)
from typing import (
    TYPE_CHECKING,
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
from ..types import (
    ConditionCallback,
)
from .abc import (
    SagaStep,
    SagaStepDecoratorMeta,
    SagaStepDecoratorWrapper,
)

if TYPE_CHECKING:
    from ..saga import (
        Saga,
    )

ConditionalSagaStepClass = TypeVar("ConditionalSagaStepClass", bound=type)


class ConditionalSagaStepDecoratorWrapper(SagaStepDecoratorWrapper):
    """TODO"""

    meta: ConditionalSagaStepDecoratorMeta


class ConditionalSagaStepDecoratorMeta(SagaStepDecoratorMeta):
    """TODO"""

    _definition: ConditionalSagaStep

    @cached_property
    def definition(self):
        """TODO"""
        if_then_alternatives = getmembers(
            self._inner,
            predicate=lambda x: isinstance(x, IfThenAlternativeWrapper) and isinstance(x.meta, IfThenAlternativeMeta),
        )
        if_then_alternatives = map(lambda member: member[1].meta.alternative, if_then_alternatives)
        if_then_alternatives = sorted(if_then_alternatives, key=attrgetter("order"))

        for alternative in if_then_alternatives:
            self._definition.if_then_alternatives.append(alternative)

        else_then_alternatives = getmembers(
            self._inner,
            predicate=(
                lambda x: isinstance(x, ElseThenAlternativeWrapper) and isinstance(x.meta, ElseThenAlternativeMeta)
            ),
        )
        else_then_alternatives = list(map(lambda member: member[1].meta.alternative, else_then_alternatives))
        if len(else_then_alternatives) > 1:
            raise MultipleElseThenException()
        elif len(else_then_alternatives) == 1:
            self._definition.else_then_alternative = else_then_alternatives[0]

        return self._definition


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

    def __call__(self, type_: ConditionalSagaStepClass) -> ConditionalSagaStepClass:
        meta = ConditionalSagaStepDecoratorMeta(type_, self)
        type_.meta = meta
        return type_

    def if_then(self, condition: ConditionCallback, saga: Saga) -> ConditionalSagaStep:
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
            "order": self.order,
            "if_then": [alternative.raw for alternative in self.if_then_alternatives],
            "else_then": None if self.else_then_alternative is None else self.else_then_alternative.raw,
        }

    def __iter__(self) -> Iterable:
        yield from (
            self.order,
            self.if_then_alternatives,
            self.else_then_alternative,
        )


@runtime_checkable
class IfThenAlternativeWrapper(Protocol):
    """TODO"""

    meta: IfThenAlternativeMeta


class IfThenAlternativeMeta:
    """TODO"""

    def __init__(self, func: ConditionCallback, alternative: IfThenAlternative):
        self._func = func
        self._alternative = alternative

    @cached_property
    def alternative(self) -> IfThenAlternative:
        """TODO"""
        self._alternative.condition = SagaOperation(self._func)
        return self._alternative


class IfThenAlternative:
    """If Then Alternative class."""

    def __init__(
        self, condition: Union[SagaOperation, ConditionCallback] = None, saga: Saga = None, order: Optional[int] = None
    ):
        if saga is None:
            raise ValueError("TODO")

        if not isinstance(condition, SagaOperation):
            condition = SagaOperation(condition)

        self.condition = condition
        self.saga = saga
        self.order = order

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

    def __call__(self, type_: ConditionCallback) -> Union[ConditionCallback, IfThenAlternativeMeta]:
        meta = IfThenAlternativeMeta(type_, self)
        type_.meta = meta
        return type_

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


@runtime_checkable
class ElseThenAlternativeWrapper(Protocol):
    """TODO"""

    meta: ElseThenAlternativeMeta


class ElseThenAlternativeMeta:
    """TODO"""

    def __init__(self, func: Callable, alternative: ElseThenAlternative):
        self._func = func
        self._alternative = alternative

    @cached_property
    def alternative(self) -> ElseThenAlternative:
        """TODO"""
        return self._alternative


class ElseThenAlternative:
    """Else Then Alternative class."""

    def __init__(self, saga: Saga = None):
        if saga is None:
            raise ValueError("TODO")

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

    def __call__(self, type_: Callable) -> Union[Callable, ElseThenAlternativeMeta]:
        meta = ElseThenAlternativeMeta(type_, self)
        type_.meta = meta
        return type_

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

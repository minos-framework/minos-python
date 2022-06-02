"""Conditional Step Definitions module."""

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
    OrderPrecedenceException,
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


@runtime_checkable
class ConditionalSagaStepDecoratorWrapper(SagaStepDecoratorWrapper, Protocol):
    """Conditional Saga Step Decorator Wrapper class."""

    meta: ConditionalSagaStepDecoratorMeta


class ConditionalSagaStepDecoratorMeta(SagaStepDecoratorMeta):
    """Conditional Saga Step Decorator Meta class."""

    _definition: ConditionalSagaStep

    @cached_property
    def definition(self):
        """Get the step definition.

        :return: A ``SagaStep`` instance.
        """
        if_then_alternatives = getmembers(
            self._inner,
            predicate=(
                lambda x: isinstance(x, IfThenAlternativeDecoratorWrapper)
                and isinstance(x.meta, IfThenAlternativeDecoratorMeta)
            ),
        )
        if_then_alternatives = list(map(lambda member: member[1].meta.alternative, if_then_alternatives))
        for alternative in if_then_alternatives:
            if alternative.order is None:
                raise OrderPrecedenceException(f"The {alternative!r} alternative does not have 'order' value.")
        if_then_alternatives.sort(key=attrgetter("order"))

        for alternative in if_then_alternatives:
            self._definition.if_then(alternative)

        else_then_alternatives = getmembers(
            self._inner,
            predicate=(
                lambda x: isinstance(x, ElseThenAlternativeDecoratorWrapper)
                and isinstance(x.meta, ElseThenAlternativeDecoratorMeta)
            ),
        )
        else_then_alternatives = list(map(lambda member: member[1].meta.alternative, else_then_alternatives))
        for alternative in else_then_alternatives:
            self._definition.else_then(alternative)

        return self._definition


TP = TypeVar("TP", bound=type)


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

    def __call__(self, type_: TP) -> Union[TP, ConditionalSagaStepDecoratorWrapper]:
        """Decorate the given type.

        :param type_: The type to be decorated.
        :return: The decorated type.
        """
        meta = ConditionalSagaStepDecoratorMeta(type_, self)
        type_.meta = meta
        return type_

    def if_then(
        self, alternative: Union[IfThenAlternative, ConditionCallback], saga: Optional[Saga] = None
    ) -> ConditionalSagaStep:
        """Add a new ``IfThenAlternative`` based on a condition and a saga.

        :param alternative: The condition that must be satisfied to execute the alternative.
        :param saga: The saga to be executed if the condition is satisfied.
        :return: This method returns the same instance that is called.
        """
        if not isinstance(alternative, IfThenAlternative):
            alternative = IfThenAlternative(saga, alternative)

        if alternative.order is None:
            if self.if_then_alternatives:
                alternative.order = self.if_then_alternatives[-1].order + 1
            else:
                alternative.order = 1

        if self.if_then_alternatives and alternative.order <= self.if_then_alternatives[-1].order:
            raise OrderPrecedenceException(
                f"Unsatisfied precedence constraints. Previous: {self.if_then_alternatives[-1].order} "
                f"Current: {alternative.order} "
            )

        self.if_then_alternatives.append(alternative)
        return self

    def else_then(self, alternative: Union[ElseThenAlternative, Saga]) -> ConditionalSagaStep:
        """Set the ``ElseThenAlternative`` with the given saga.

        :param alternative: The saga to be executed if not any condition is met.
        :return: This method returns the same instance that is called.
        """

        if self.else_then_alternative is not None:
            raise MultipleElseThenException()

        if not isinstance(alternative, ElseThenAlternative):
            alternative = ElseThenAlternative(alternative)

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
class IfThenAlternativeDecoratorWrapper(Protocol):
    """If Then Alternative Decorator Wrapper class."""

    meta: IfThenAlternativeDecoratorMeta
    __call__: ConditionCallback


class IfThenAlternativeDecoratorMeta:
    """If Then Alternative Decorator Meta class."""

    def __init__(self, inner: ConditionCallback, alternative: IfThenAlternative):
        self._inner = inner
        self._alternative = alternative

    @cached_property
    def alternative(self) -> IfThenAlternative:
        """Get the if-then alternative.

        :return: A ``IfThenAlternative`` instance.
        """
        self._alternative.condition = SagaOperation(self._inner)
        return self._alternative


class IfThenAlternative:
    """If Then Alternative class."""

    def __init__(
        self,
        saga: Saga,
        condition: Optional[Union[ConditionCallback, SagaOperation[ConditionCallback]]] = None,
        order: Optional[int] = None,
    ):
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

    def __call__(self, func: ConditionCallback) -> IfThenAlternativeDecoratorWrapper:
        meta = IfThenAlternativeDecoratorMeta(func, self)
        func.meta = meta
        return func

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
            "order": self.order,
            "condition": self.condition.raw,
            "saga": self.saga.raw,
        }

    def __repr__(self) -> str:
        return f"{type(self).__name__}{tuple(self)}"

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, type(self)) and tuple(self) == tuple(other)

    def __iter__(self):
        yield from (
            self.condition,
            self.saga,
        )


@runtime_checkable
class ElseThenAlternativeDecoratorWrapper(Protocol):
    """Else Then Alternative Decorator Wrapper class."""

    meta: ElseThenAlternativeDecoratorMeta
    __call__: Callable


class ElseThenAlternativeDecoratorMeta:
    """Else Then Alternative Decorator Meta class."""

    def __init__(self, inner: Callable, alternative: ElseThenAlternative):
        self._inner = inner
        self._alternative = alternative

    @cached_property
    def alternative(self) -> ElseThenAlternative:
        """Get the else-then alternative.

        :return: A ``ElseThenAlternative`` instance.
        """
        return self._alternative


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

    def __call__(self, func: Callable) -> ElseThenAlternativeDecoratorWrapper:
        meta = ElseThenAlternativeDecoratorMeta(func, self)
        func.meta = meta
        return func

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

    def __repr__(self) -> str:
        return f"{type(self).__name__}{tuple(self)}"

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, type(self)) and tuple(self) == tuple(other)

    def __iter__(self):
        yield from (self.saga,)

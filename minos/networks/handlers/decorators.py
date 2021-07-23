# Copyright (C) 2020 Clariteia SL
#
# This file is part of minos framework.
#
# Minos framework can not be copied and/or distributed without the express
# permission of Clariteia SL.
from __future__ import (
    annotations,
)

from abc import (
    abstractmethod,
)
from enum import (
    Enum,
    auto,
)
from inspect import (
    getmembers,
    iscoroutinefunction,
    isfunction,
)
from typing import (
    Callable,
    Final,
    Iterable,
    Type,
    Union,
)

from minos.common import (
    import_module,
)

from ..exceptions import (
    MinosMultipleEnrouteDecoratorKindsException,
)


class EnrouteDecorator:
    """Base Decorator class."""

    # noinspection PyFinal
    KIND: Final[EnrouteDecoratorKind]

    def __call__(self, fn: Callable):
        if iscoroutinefunction(fn):

            async def _wrapper(*args, **kwargs):
                return await fn(*args, **kwargs)

        else:

            def _wrapper(*args, **kwargs):
                return fn(*args, **kwargs)

        _wrapper.__decorators__ = {self} | getattr(fn, "__decorators__", set())
        kinds = set(decorator.KIND for decorator in _wrapper.__decorators__)
        if len(kinds) > 1:
            raise MinosMultipleEnrouteDecoratorKindsException(
                f"There are multiple kinds but only one is allowed: {kinds}"
            )

        _wrapper.__base_func__ = getattr(fn, "__base_func__", fn)

        return _wrapper

    def __repr__(self):
        args = ", ".join(map(repr, self))
        return f"{type(self).__name__}({args})"

    def __eq__(self, other: EnrouteDecorator) -> bool:
        return type(self) == type(other) and tuple(self) == tuple(other)

    def __hash__(self) -> int:
        return hash(tuple(self))

    @abstractmethod
    def __iter__(self) -> Iterable:
        raise NotImplementedError


class EnrouteDecoratorKind(Enum):
    """Enroute Kind enumerate."""

    Command = auto()
    Event = auto()
    Query = auto()


class BrokerCommandEnrouteDecorator(EnrouteDecorator):
    """Broker Command Enroute class"""

    KIND: Final[EnrouteDecoratorKind] = EnrouteDecoratorKind.Command

    def __init__(self, topics: Iterable[str]):
        if isinstance(topics, str):
            topics = (topics,)
        self.topics = tuple(topics)

    def __iter__(self) -> Iterable:
        yield from (self.topics,)


class BrokerQueryEnrouteDecorator(EnrouteDecorator):
    """Broker Query Enroute class"""

    KIND: Final[EnrouteDecoratorKind] = EnrouteDecoratorKind.Query

    def __init__(self, topics: Iterable[str]):
        if isinstance(topics, str):
            topics = (topics,)
        self.topics = tuple(topics)

    def __iter__(self) -> Iterable:
        yield from (self.topics,)


class BrokerEventEnrouteDecorator(EnrouteDecorator):
    """Broker Event Enroute class"""

    KIND: Final[EnrouteDecoratorKind] = EnrouteDecoratorKind.Event

    def __init__(self, topics: Iterable[str]):
        if isinstance(topics, str):
            topics = (topics,)
        self.topics = tuple(topics)

    def __iter__(self) -> Iterable:
        yield from (self.topics,)


class BrokerEnroute:
    """Broker Enroute class"""

    command = BrokerCommandEnrouteDecorator
    query = BrokerQueryEnrouteDecorator
    event = BrokerEventEnrouteDecorator


class RestCommandEnrouteDecorator(EnrouteDecorator):
    """Rest Command Enroute class"""

    KIND: Final[EnrouteDecoratorKind] = EnrouteDecoratorKind.Command

    def __init__(self, url: str, method: str):
        self.url = url
        self.method = method

    def __iter__(self) -> Iterable:
        yield from (
            self.url,
            self.method,
        )


class RestQueryEnrouteDecorator(EnrouteDecorator):
    """Rest Query Enroute class"""

    KIND: Final[EnrouteDecoratorKind] = EnrouteDecoratorKind.Query

    def __init__(self, url: str, method: str):
        self.url = url
        self.method = method

    def __iter__(self) -> Iterable:
        yield from (
            self.url,
            self.method,
        )


class RestEnroute:
    """Rest Enroute class"""

    command = RestCommandEnrouteDecorator
    query = RestQueryEnrouteDecorator


class Enroute:
    """Enroute decorator main class"""

    broker = BrokerEnroute
    rest = RestEnroute


enroute = Enroute


class EnrouteDecoratorAnalyzer:
    """Search decorators in specified class"""

    def __init__(self, decorated: Union[str, Type]):
        if isinstance(decorated, str):
            decorated = import_module(decorated)

        self.decorated = decorated
        self._result = None

    def rest(self) -> dict[Callable, set[EnrouteDecorator]]:
        """Returns rest values.

        :return: A mapping with functions as keys and a sets of decorators as values.
        """
        return self._get_items({RestCommandEnrouteDecorator, RestQueryEnrouteDecorator})

    def command(self) -> dict[Callable, set[EnrouteDecorator]]:
        """Returns command values.

        :return: A mapping with functions as keys and a sets of decorators as values.
        """
        return self._get_items({BrokerCommandEnrouteDecorator, BrokerQueryEnrouteDecorator})

    def event(self) -> dict[Callable, set[EnrouteDecorator]]:
        """Returns event values.

        :return: A mapping with functions as keys and a sets of decorators as values.
        """
        return self._get_items({BrokerEventEnrouteDecorator})

    def _get_items(self, expected_types: set[Type[EnrouteDecorator]]) -> dict[Callable, set[EnrouteDecorator]]:
        items = dict()
        for fn, decorators in self.get_all().items():
            decorators = {decorator for decorator in decorators if type(decorator) in expected_types}
            if len(decorators):
                items[fn] = decorators
        return items

    def get_all(self) -> dict[Callable, set[EnrouteDecorator]]:
        """Get all functions decorated with enroute decorators.

        :return: A mapping with functions as keys and a sets of decorators as values.
        """
        if self._result is None:

            result = dict()
            for _, fn in getmembers(self.decorated, predicate=isfunction):
                if not hasattr(fn, "__decorators__"):
                    continue
                result[fn] = fn.__decorators__
            return result

        return self._result

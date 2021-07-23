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
    Awaitable,
    Callable,
    Final,
    Iterable,
    Optional,
    Type,
    Union,
)

from minos.common import (
    Request,
    Response,
    import_module,
)

from ..exceptions import (
    MinosMultipleEnrouteDecoratorKindsException,
)

Adapter = Callable[[Request], Union[Optional[Response], Awaitable[Optional[Response]]]]


class EnrouteDecorator:
    """Base Decorator class."""

    # noinspection PyFinal
    KIND: Final[EnrouteDecoratorKind]

    def __call__(self, fn: Adapter) -> Adapter:
        if iscoroutinefunction(fn):

            async def _wrapper(*args, **kwargs) -> Optional[Response]:
                return await fn(*args, **kwargs)

        else:

            def _wrapper(*args, **kwargs) -> Optional[Response]:
                return fn(*args, **kwargs)

        _wrapper.__decorators__ = getattr(fn, "__decorators__", set())
        _wrapper.__decorators__.add(self)
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
    Query = auto()
    Event = auto()

    @property
    def pref_fn_name(self) -> str:
        """TODO

        :return:TODO
        """
        mapping = {
            self.Command: "_pre_command_handle",
            self.Query: "_pre_query_handle",
            self.Event: "_pre_event_handle",
        }
        return mapping[self]


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

    def get_rest_command_query(self) -> dict[str, set[EnrouteDecorator]]:
        """Returns rest values.

        :return: A mapping with functions as keys and a sets of decorators as values.
        """
        return self._get_items({RestCommandEnrouteDecorator, RestQueryEnrouteDecorator})

    def get_broker_command_query(self) -> dict[str, set[EnrouteDecorator]]:
        """Returns command values.

        :return: A mapping with functions as keys and a sets of decorators as values.
        """
        return self._get_items({BrokerCommandEnrouteDecorator, BrokerQueryEnrouteDecorator})

    def get_broker_event(self) -> dict[str, set[EnrouteDecorator]]:
        """Returns event values.

        :return: A mapping with functions as keys and a sets of decorators as values.
        """
        return self._get_items({BrokerEventEnrouteDecorator})

    def _get_items(self, expected_types: set[Type[EnrouteDecorator]]) -> dict[str, set[EnrouteDecorator]]:
        items = dict()
        for fn, decorators in self.get_all().items():
            decorators = {decorator for decorator in decorators if type(decorator) in expected_types}
            if len(decorators):
                items[fn] = decorators
        return items

    def get_all(self) -> dict[str, set[EnrouteDecorator]]:
        """Get all functions decorated with enroute decorators.

        :return: A mapping with functions as keys and a sets of decorators as values.
        """
        result = dict()
        for name, fn in getmembers(self.decorated, predicate=isfunction):
            if not hasattr(fn, "__decorators__"):
                continue
            result[name] = fn.__decorators__
        return result


class EnrouteBuilder:
    """TODO"""

    def __init__(self, decorated: Union[str, Type]):
        if isinstance(decorated, str):
            decorated = import_module(decorated)

        self.analyzer = EnrouteDecoratorAnalyzer(decorated)
        self.instance = decorated()

    def get_rest_command_query(self) -> list[(Callable, EnrouteDecorator)]:
        """TODO

        :return: TODO
        """
        mapping = self.analyzer.get_rest_command_query()

        ans = list()
        for name, decorators in mapping.items():
            fn = getattr(self.instance, name)
            for decorator in decorators:
                pre_fn = getattr(self.instance, decorator.KIND.pref_fn_name, None)
                if pre_fn is not None:

                    async def fn(request):
                        request = await pre_fn(request)
                        return await fn(request)

                ans.append((fn, decorator))
        return ans

    def get_broker_command_query(self) -> list[(Callable, EnrouteDecorator)]:
        """TODO

        :return: TODO
        """
        mapping = self.analyzer.get_broker_command_query()
        ans = list()
        for name, decorators in mapping.items():
            fn = getattr(self.instance, name)
            for decorator in decorators:
                pre_fn = getattr(self.instance, decorator.KIND.pref_fn_name, None)
                if pre_fn is not None:

                    async def fn(request):
                        request = await pre_fn(request)
                        return await fn(request)

                ans.append((fn, decorator))
        return ans

    def get_broker_event(self) -> list[(Callable, EnrouteDecorator)]:
        """TODO

        :return: TODO
        """
        mapping = self.analyzer.get_broker_event()
        ans = list()
        for name, decorators in mapping.items():
            fn = getattr(self.instance, name)
            for decorator in decorators:
                pre_fn = getattr(self.instance, decorator.KIND.pref_fn_name, None)
                if pre_fn is not None:

                    async def _fn(request):
                        request = await pre_fn(request)
                        return await fn(request)

                else:
                    _fn = fn
                ans.append((_fn, decorator))
        return ans

"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from inspect import (
    iscoroutinefunction,
)
from typing import (
    Awaitable,
    Callable,
    Optional,
    Type,
    Union,
)

from minos.common import (
    import_module,
)

from ..exceptions import (
    MinosRedefinedEnrouteDecoratorException,
)
from ..messages import (
    Request,
    Response,
)
from .analyzers import (
    EnrouteAnalyzer,
)
from .definitions import (
    BrokerEnrouteDecorator,
    EnrouteDecorator,
    RestEnrouteDecorator,
)


class EnrouteBuilder:
    """Enroute builder class."""

    def __init__(self, decorated: Union[str, Type], *args, **kwargs):
        if isinstance(decorated, str):
            decorated = import_module(decorated)

        self.decorated = decorated
        self.analyzer = EnrouteAnalyzer(decorated, *args, **kwargs)

    def get_rest_command_query(self) -> dict[RestEnrouteDecorator, Callable[[Request], Awaitable[Response]]]:
        """Get the rest handlers for commands and queries.

        :return: A dictionary with decorator classes as keys and callable handlers as values.
        """
        mapping = self.analyzer.get_rest_command_query()
        # noinspection PyTypeChecker
        return self._build(mapping)

    def get_broker_command_query(self) -> dict[BrokerEnrouteDecorator, Callable[[Request], Awaitable[Response]]]:
        """Get the broker handlers for commands and queries.

        :return: A dictionary with decorator classes as keys and callable handlers as values.
        """
        mapping = self.analyzer.get_broker_command_query()
        # noinspection PyTypeChecker
        return self._build(mapping)

    def get_broker_event(self) -> dict[BrokerEnrouteDecorator, Callable[[Request], Awaitable[Response]]]:
        """Get the broker handlers for events.

        :return: A dictionary with decorator classes as keys and callable handlers as values.
        """
        mapping = self.analyzer.get_broker_event()
        # noinspection PyTypeChecker
        return self._build(mapping)

    def _build(
        self, mapping: dict[str, set[EnrouteDecorator]]
    ) -> dict[EnrouteDecorator, Callable[[Request], Awaitable[Response]]]:

        ans = dict()
        for name, decorators in mapping.items():
            for decorator in decorators:
                if decorator in ans:
                    raise MinosRedefinedEnrouteDecoratorException(f"{decorator!r} can be used only once.")
                ans[decorator] = self._build_one(name, decorator.pre_fn_name)
        return ans

    def _build_one(self, name: str, pref_fn_name: str) -> Callable:
        instance = self.decorated()
        fn = getattr(instance, name)
        pre_fn = getattr(instance, pref_fn_name, None)

        if iscoroutinefunction(fn):
            _awaitable_fn = fn
        else:

            async def _awaitable_fn(request: Request) -> Optional[Response]:
                return fn(request)

        if pre_fn is None:
            _wrapped_fn = _awaitable_fn
        else:
            if iscoroutinefunction(pre_fn):

                async def _wrapped_fn(request: Request) -> Optional[Response]:
                    request = await pre_fn(request)
                    return await _awaitable_fn(request)

            else:

                async def _wrapped_fn(request: Request) -> Optional[Response]:
                    request = pre_fn(request)
                    return await _awaitable_fn(request)

        return _wrapped_fn

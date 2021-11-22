from asyncio import (
    gather,
)
from collections import (
    defaultdict,
)
from functools import (
    partial,
)
from inspect import (
    isawaitable,
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
from ..requests import (
    Request,
    Response,
)
from .analyzers import (
    EnrouteAnalyzer,
)
from .definitions import (
    BrokerEnrouteDecorator,
    EnrouteDecorator,
    EnrouteDecoratorKind,
    PeriodicEnrouteDecorator,
    RestEnrouteDecorator,
)

Handler = Callable[[Request], Awaitable[Optional[Response]]]


class EnrouteBuilder:
    """Enroute builder class."""

    def __init__(
        self, *classes: Union[str, Type], middleware: Optional[Union[str, Callable, list[Union[str, Callable]]]] = None
    ):
        if middleware is None:
            middleware = tuple()
        if not isinstance(middleware, (tuple, list)):
            middleware = [middleware]

        middleware = list(map(lambda fn: fn if not isinstance(fn, str) else import_module(fn), middleware))

        classes = tuple((class_ if not isinstance(class_, str) else import_module(class_)) for class_ in classes)

        self.classes = classes
        self.middleware = middleware

    def get_rest_command_query(self, **kwargs) -> dict[RestEnrouteDecorator, Handler]:
        """Get the rest handlers for commands and queries.

        :return: A dictionary with decorator classes as keys and callable handlers as values.
        """
        # noinspection PyTypeChecker
        return self._build("get_rest_command_query", **kwargs)

    def get_broker_command_query_event(self, **kwargs) -> dict[BrokerEnrouteDecorator, Handler]:
        """Get the broker handlers for commands, queries and events.

        :return: A dictionary with decorator classes as keys and callable handlers as values.
        """
        # noinspection PyTypeChecker
        return self._build("get_broker_command_query_event", **kwargs)

    def get_broker_command_query(self, **kwargs) -> dict[BrokerEnrouteDecorator, Handler]:
        """Get the broker handlers for commands and queries.

        :return: A dictionary with decorator classes as keys and callable handlers as values.
        """
        # noinspection PyTypeChecker
        return self._build("get_broker_command_query", **kwargs)

    def get_broker_event(self, **kwargs) -> dict[BrokerEnrouteDecorator, Handler]:
        """Get the broker handlers for events.

        :return: A dictionary with decorator classes as keys and callable handlers as values.
        """
        # noinspection PyTypeChecker
        return self._build("get_broker_event", **kwargs)

    def get_periodic_event(self, **kwargs) -> dict[PeriodicEnrouteDecorator, Handler]:
        """Get the periodic handlers for events.

        :return: A dictionary with decorator classes as keys and callable handlers as values.
        """
        # noinspection PyTypeChecker
        return self._build("get_periodic_event", **kwargs)

    def _build(self, method_name: str, **kwargs) -> dict[EnrouteDecorator, Handler]:
        def _flatten(decorator: EnrouteDecorator, fns: set[Handler]) -> Handler:
            if len(fns) == 1:
                return next(iter(fns))

            if decorator.KIND != EnrouteDecoratorKind.Event:
                raise MinosRedefinedEnrouteDecoratorException(f"{decorator!r} can be used only once.")

            async def _fn(*ag, **kw):
                return await gather(*(fn(*ag, **kw) for fn in fns))

            return _fn

        return {
            decorator: _flatten(decorator, fns)
            for decorator, fns in self._build_all_classes(method_name, **kwargs).items()
        }

    def _build_all_classes(self, method_name: str, **kwargs) -> dict[EnrouteDecorator, set[Handler]]:
        decomposed_handlers = defaultdict(set)
        for class_ in self.classes:
            self._build_one_class(class_, method_name, decomposed_handlers, **kwargs)
        return decomposed_handlers

    def _build_one_class(
        self, class_: type, method_name: str, ans: dict[EnrouteDecorator, set[Handler]], **kwargs
    ) -> None:
        analyzer = EnrouteAnalyzer(class_, **kwargs)
        mapping = getattr(analyzer, method_name)()

        for name, decorators in mapping.items():
            for decorator in decorators:
                ans[decorator].add(self._build_one_method(class_, name, decorator.pre_fn_name, decorator.post_fn_name))

    def _build_one_method(self, class_: type, name: str, pref_fn_name: str, post_fn_name: str, **kwargs) -> Handler:
        instance = class_(**kwargs)
        fn = getattr(instance, name)
        pre_fn = getattr(instance, pref_fn_name, None)
        post_fn = getattr(instance, post_fn_name, None)

        async def _wrapped_fn(request: Request) -> Optional[Response]:
            if pre_fn is not None:
                request = pre_fn(request)
                if isawaitable(request):
                    request = await request

            response = fn(request)
            if isawaitable(response):
                response = await response

            if post_fn is not None:
                response = post_fn(response)
                if isawaitable(response):
                    response = await response

            return response

        for middleware_fn in reversed(self.middleware):
            _wrapped_fn = partial(middleware_fn, inner=_wrapped_fn)

        return _wrapped_fn

from __future__ import (
    annotations,
)

import logging
from asyncio import (
    CancelledError,
)
from functools import (
    wraps,
)
from inspect import (
    isawaitable,
)
from typing import (
    Any,
    Awaitable,
    Callable,
    Optional,
    Union,
)

from dependency_injector.wiring import (
    Provide,
    inject,
)

from minos.common import (
    MinosConfig,
    MinosSetup,
    NotProvidedException,
)

from ...decorators import (
    EnrouteBuilder,
)
from ...exceptions import (
    MinosActionNotFoundException,
)
from ...requests import (
    REQUEST_USER_CONTEXT_VAR,
    Request,
    Response,
    ResponseException,
)
from ..messages import (
    REQUEST_HEADERS_CONTEXT_VAR,
    BrokerMessage,
    BrokerMessageStatus,
)
from ..publishers import (
    BrokerPublisher,
)
from .entries import (
    BrokerHandlerEntry,
)
from .requests import (
    BrokerRequest,
    BrokerResponse,
)

logger = logging.getLogger(__name__)


class BrokerDispatcher(MinosSetup):
    """TODO"""

    def __init__(
        self, handlers: dict[str, Optional[Callable]], publisher: BrokerPublisher, **kwargs,
    ):
        super().__init__(**kwargs)
        self._handlers = handlers
        self._publisher = publisher

    @classmethod
    def _from_config(cls, config: MinosConfig, **kwargs) -> BrokerDispatcher:
        kwargs["handlers"] = cls._get_handlers(config, **kwargs)
        kwargs["publisher"] = cls._get_publisher(**kwargs)
        # noinspection PyProtectedMember
        return cls(**config.broker.queue._asdict(), **kwargs)

    @staticmethod
    def _get_handlers(
        config: MinosConfig, handlers: dict[str, Optional[Callable]] = None, **kwargs
    ) -> dict[str, Callable[[BrokerRequest], Awaitable[Optional[BrokerResponse]]]]:
        if handlers is None:
            builder = EnrouteBuilder(*config.services, middleware=config.middleware)
            decorators = builder.get_broker_command_query_event(config=config, **kwargs)
            handlers = {decorator.topic: fn for decorator, fn in decorators.items()}
        return handlers

    # noinspection PyUnusedLocal
    @staticmethod
    @inject
    def _get_publisher(
        publisher: Optional[BrokerPublisher] = None,
        broker_publisher: BrokerPublisher = Provide["broker_publisher"],
        **kwargs,
    ) -> BrokerPublisher:
        if publisher is None:
            publisher = broker_publisher
        if publisher is None or isinstance(publisher, Provide):
            raise NotProvidedException(f"A {BrokerPublisher!r} object must be provided.")
        return publisher

    @property
    def publisher(self) -> BrokerPublisher:
        """Get the publisher instance.

        :return: A ``BrokerPublisher`` instance.
        """
        return self._publisher

    @property
    def handlers(self) -> dict[str, Optional[Callable]]:
        """Handlers getter.

        :return: A dictionary in which the keys are topics and the values are the handler.
        """
        return self._handlers

    async def dispatch(self, entry: BrokerHandlerEntry) -> None:
        """TODO"""
        logger.info(f"Dispatching '{entry!r}'...")
        try:
            await self._dispatch(entry)
        except (CancelledError, Exception) as exc:
            logger.warning(f"Raised an exception while dispatching {entry!r}: {exc!r}")
            entry.exception = exc
            if isinstance(exc, CancelledError):
                raise exc

    async def _dispatch(self, entry: BrokerHandlerEntry) -> None:
        """TODO"""

        handler = self.get_handler(entry.topic)
        fn = self.get_callback(handler)

        message = entry.data
        data, status, headers = await fn(message)

        if message.reply_topic is not None:
            await self.publisher.send(
                data,
                topic=message.reply_topic,
                identifier=message.identifier,
                status=status,
                user=message.user,
                headers=headers,
            )

    @staticmethod
    def get_callback(
        fn: Callable[[BrokerRequest], Union[Optional[BrokerResponse], Awaitable[Optional[BrokerResponse]]]]
    ) -> Callable[[BrokerMessage], Awaitable[tuple[Any, BrokerMessageStatus, dict[str, str]]]]:
        """Get the handler function to be used by the Broker Handler.

        :param fn: The action function.
        :return: A wrapper function around the given one that is compatible with the Broker Handler API.
        """

        @wraps(fn)
        async def _wrapper(raw: BrokerMessage) -> tuple[Any, BrokerMessageStatus, dict[str, str]]:
            request = BrokerRequest(raw)
            user_token = REQUEST_USER_CONTEXT_VAR.set(request.user)
            headers_token = REQUEST_HEADERS_CONTEXT_VAR.set(raw.headers)

            try:
                response = fn(request)
                if isawaitable(response):
                    response = await response
                if isinstance(response, Response):
                    response = await response.content()
                return response, BrokerMessageStatus.SUCCESS, REQUEST_HEADERS_CONTEXT_VAR.get()
            except ResponseException as exc:
                logger.warning(f"Raised an application exception: {exc!s}")
                return repr(exc), BrokerMessageStatus.ERROR, REQUEST_HEADERS_CONTEXT_VAR.get()
            except Exception as exc:
                logger.exception(f"Raised a system exception: {exc!r}")
                return repr(exc), BrokerMessageStatus.SYSTEM_ERROR, REQUEST_HEADERS_CONTEXT_VAR.get()
            finally:
                REQUEST_USER_CONTEXT_VAR.reset(user_token)
                REQUEST_HEADERS_CONTEXT_VAR.reset(headers_token)

        return _wrapper

    def get_handler(
        self, topic: str
    ) -> Optional[Callable[[Request], Union[Optional[Response], Awaitable[Optional[Response]]]]]:
        """Get handling function to be called.

        Gets the instance of the class and method to call.

        Args:
            topic: Kafka topic. Example: "TicketAdded"

        Raises:
            MinosNetworkException: topic TicketAdded have no controller/action configured, please review th
                configuration file.
        """
        if topic not in self._handlers:
            raise MinosActionNotFoundException(
                f"topic {topic} have no controller/action configured, " f"please review th configuration file"
            )

        handler = self._handlers[topic]

        logger.debug(f"Loaded {handler!r} action!")
        return handler

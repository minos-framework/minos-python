from __future__ import (
    annotations,
)

import logging
import traceback
from collections.abc import (
    Awaitable,
    Callable,
)
from functools import (
    wraps,
)
from inspect import (
    isawaitable,
)
from typing import (
    Optional,
    Union,
)

from minos.common import (
    Config,
    Inject,
    NotProvidedException,
    SetupMixin,
)

from ...decorators import (
    EnrouteFactory,
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
    BrokerMessageV1,
    BrokerMessageV1Payload,
    BrokerMessageV1Status,
)
from ..publishers import (
    BrokerPublisher,
)
from .requests import (
    BrokerRequest,
    BrokerResponse,
)

logger = logging.getLogger(__name__)


class BrokerDispatcher(SetupMixin):
    """Broker Dispatcher class."""

    def __init__(self, actions: dict[str, Optional[Callable]], publisher: BrokerPublisher, **kwargs):
        super().__init__(**kwargs)
        self._actions = actions
        self._publisher = publisher

    @classmethod
    def _from_config(cls, config: Config, **kwargs) -> BrokerDispatcher:
        kwargs["actions"] = cls._get_actions(config, **kwargs)
        kwargs["publisher"] = cls._get_publisher(**kwargs)
        # noinspection PyProtectedMember
        return cls(**kwargs)

    @staticmethod
    def _get_actions(
        config: Config, handlers: dict[str, Optional[Callable]] = None, **kwargs
    ) -> dict[str, Callable[[BrokerRequest], Awaitable[Optional[BrokerResponse]]]]:
        if handlers is None:
            builder = EnrouteFactory(*config.get_services(), middleware=config.get_middleware())
            decorators = builder.get_broker_command_query_event(config=config, **kwargs)
            handlers = {decorator.topic: fn for decorator, fn in decorators.items()}
        return handlers

    # noinspection PyUnusedLocal
    @staticmethod
    @Inject()
    def _get_publisher(
        publisher: Optional[BrokerPublisher] = None,
        broker_publisher: BrokerPublisher = None,
        **kwargs,
    ) -> BrokerPublisher:
        if publisher is None:
            publisher = broker_publisher
        if publisher is None:
            raise NotProvidedException(f"A {BrokerPublisher!r} object must be provided.")
        return publisher

    @property
    def publisher(self) -> BrokerPublisher:
        """Get the publisher instance.

        :return: A ``BrokerPublisher`` instance.
        """
        return self._publisher

    @property
    def actions(self) -> dict[str, Optional[Callable]]:
        """Actions getter.

        :return: A dictionary in which the keys are topics and the values are the handler.
        """
        return self._actions

    async def dispatch(self, message: BrokerMessage) -> None:
        """Dispatch an entry.

        :param message: The entry to be dispatched.
        :return: This method does not return anything.
        """
        action = self.get_action(message.topic)
        fn = self.get_callback(action)

        payload = await fn(message)

        if message.should_reply:
            reply = BrokerMessageV1(topic=message.reply_topic, payload=payload, identifier=message.identifier)
            await self.publisher.send(reply)

    @staticmethod
    def get_callback(
        fn: Callable[[BrokerRequest], Union[Optional[BrokerResponse], Awaitable[Optional[BrokerResponse]]]]
    ) -> Callable[[BrokerMessage], Awaitable[BrokerMessageV1Payload]]:
        """Get the handler function to be used by the Broker Handler.

        :param fn: The action function.
        :return: A wrapper function around the given one that is compatible with the Broker Handler API.
        """

        @wraps(fn)
        async def _wrapper(raw: BrokerMessage) -> BrokerMessageV1Payload:
            logger.info(f"Dispatching '{raw!s}'...")

            request = BrokerRequest(raw)
            user_token = REQUEST_USER_CONTEXT_VAR.set(request.user)
            headers_token = REQUEST_HEADERS_CONTEXT_VAR.set(request.headers)

            try:
                response = fn(request)
                if isawaitable(response):
                    response = await response
                if isinstance(response, Response):
                    content, status = await response.content(), response.status
                else:
                    content, status = None, BrokerMessageV1Status.SUCCESS
            except ResponseException as exc:
                tb = traceback.format_exc()
                logger.error(f"Raised an application exception:\n {tb}")
                content, status = repr(exc), exc.status
            except Exception as exc:
                tb = traceback.format_exc()
                logger.exception(f"Raised a system exception:\n {tb}")
                content, status = repr(exc), BrokerMessageV1Status.SYSTEM_ERROR
            finally:
                headers = REQUEST_HEADERS_CONTEXT_VAR.get()
                REQUEST_USER_CONTEXT_VAR.reset(user_token)
                REQUEST_HEADERS_CONTEXT_VAR.reset(headers_token)

            return BrokerMessageV1Payload(content=content, status=status, headers=headers)

        return _wrapper

    def get_action(self, topic: str) -> Callable[[Request], Union[Optional[Response], Awaitable[Optional[Response]]]]:
        """Get the handling function to be called.

        :param topic: The name of the topic that matches the action.
        :return: A ``Callable`` instance.
        """
        if topic not in self._actions:
            raise MinosActionNotFoundException(
                f"topic {topic} have no controller/action configured, " f"please review th configuration file"
            )

        action = self._actions[topic]

        logger.debug(f"Loaded {action!r} action!")
        return action

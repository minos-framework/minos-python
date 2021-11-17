from __future__ import (
    annotations,
)

import logging
from inspect import (
    isawaitable,
)
from typing import (
    Any,
    Awaitable,
    Callable,
    Optional,
    Tuple,
    Union,
)

from dependency_injector.wiring import (
    Provide,
    inject,
)

from minos.common import (
    MinosConfig,
)

from ....decorators import (
    EnrouteBuilder,
)
from ....requests import (
    USER_CONTEXT_VAR,
    Response,
    ResponseException,
)
from ...messages import (
    Command,
    CommandStatus,
)
from ...publishers import (
    CommandReplyBroker,
)
from ..abc import (
    Handler,
)
from ..entries import (
    HandlerEntry,
)
from ..requests import (
    HandlerRequest,
)

logger = logging.getLogger(__name__)


class CommandHandler(Handler):
    """Command Handler class."""

    @inject
    def __init__(self, command_reply_broker: CommandReplyBroker = Provide["command_reply_broker"], **kwargs: Any):
        super().__init__(**kwargs)
        self.command_reply_broker = command_reply_broker

    @classmethod
    def _from_config(cls, *args, config: MinosConfig, **kwargs) -> CommandHandler:
        handlers = cls._handlers_from_config(config, **kwargs)
        # noinspection PyProtectedMember
        return cls(handlers=handlers, **config.broker.queue._asdict(), **kwargs)

    @staticmethod
    def _handlers_from_config(config: MinosConfig, **kwargs) -> dict[str, Callable[[HandlerRequest], Awaitable]]:
        builder = EnrouteBuilder(*config.services, middleware=config.middleware)
        decorators = builder.get_broker_command_query(config=config, **kwargs)
        handlers = {decorator.topic: fn for decorator, fn in decorators.items()}
        return handlers

    async def dispatch_one(self, entry: HandlerEntry) -> None:
        """Dispatch one row.

        :param entry: Entry to be dispatched.
        :return: This method does not return anything.
        """
        logger.info(f"Dispatching '{entry!s}'...")

        fn = self.get_callback(entry.callback)
        command = entry.data
        items, status = await fn(command)

        await self.command_reply_broker.send(items, topic=command.reply_topic, saga=command.saga, status=status)

    @staticmethod
    def get_callback(
        fn: Callable[[HandlerRequest], Union[Optional[HandlerRequest], Awaitable[Optional[HandlerRequest]]]]
    ) -> Callable[[Command], Awaitable[Tuple[Any, CommandStatus]]]:
        """Get the handler function to be used by the Command Handler.

        :param fn: The action function.
        :return: A wrapper function around the given one that is compatible with the Command Handler API.
        """

        async def _fn(raw: Command) -> Tuple[Any, CommandStatus]:
            request = HandlerRequest(raw)
            token = USER_CONTEXT_VAR.set(request.user)

            try:
                response = fn(request)
                if isawaitable(response):
                    response = await response
                if isinstance(response, Response):
                    response = await response.content()
                return response, CommandStatus.SUCCESS
            except ResponseException as exc:
                logger.warning(f"Raised an application exception: {exc!s}")
                return repr(exc), CommandStatus.ERROR
            except Exception as exc:
                logger.exception(f"Raised a system exception: {exc!r}")
                return repr(exc), CommandStatus.SYSTEM_ERROR
            finally:
                USER_CONTEXT_VAR.reset(token)

        return _fn

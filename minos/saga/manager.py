"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

import logging
from typing import (
    NoReturn,
    Optional,
    Union,
)
from uuid import (
    UUID,
)

from dependency_injector.wiring import (
    Provide,
)

from minos.common import (
    CommandReply,
    MinosConfig,
    MinosHandler,
    MinosHandlerNotProvidedException,
    MinosPool,
    MinosSagaManager,
    import_module,
)

from .context import (
    SagaContext,
)
from .definitions import (
    Saga,
)
from .exceptions import (
    MinosSagaFailedExecutionException,
    MinosSagaPausedExecutionStepException,
)
from .executions import (
    SagaExecution,
    SagaExecutionStorage,
    SagaStatus,
)

logger = logging.getLogger(__name__)


def _build_definitions(items) -> dict[str, Saga]:
    def _fn(item) -> Saga:
        controller = import_module(item.controller)
        return getattr(controller, item.action)

    return {item.name: _fn(item) for item in items}


class SagaManager(MinosSagaManager):
    """Saga Manager implementation class.

    The purpose of this class is to manage the running process for new or paused``SagaExecution`` instances.
    """

    reply_pool: MinosPool[MinosHandler] = Provide["reply_pool"]

    def __init__(
        self,
        storage: SagaExecutionStorage,
        definitions: dict[str, Saga],
        reply_pool: Optional[MinosPool[MinosHandler]] = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.storage = storage
        self.definitions = definitions

        if reply_pool is not None:
            self.reply_pool = reply_pool

        if self.reply_pool is None or isinstance(self.reply_pool, Provide):
            raise MinosHandlerNotProvidedException("A handler pool instance is required.")

    @classmethod
    def _from_config(cls, *args, config: MinosConfig, **kwargs) -> SagaManager:
        """Build an instance from config.

        :param args: Additional positional arguments.
        :param config: Config instance.
        :param kwargs: Additional named arguments.
        :return: A new ``classmethod`` instance.
        """
        storage = SagaExecutionStorage.from_config(config=config, **kwargs)
        definitions = _build_definitions(config.saga.items)
        return cls(*args, storage=storage, definitions=definitions, **kwargs)

    async def _run_new(
        self,
        name: Optional[str] = None,
        context: Optional[SagaContext] = None,
        definition: Optional[Saga] = None,
        **kwargs,
    ) -> Union[UUID, SagaExecution]:
        if definition is None:
            definition = self.definitions.get(name)
        execution = SagaExecution.from_saga(definition, context=context)
        return await self._run(execution, **kwargs)

    async def _load_and_run(self, reply: CommandReply, **kwargs) -> Union[UUID, SagaExecution]:
        execution = self.storage.load(reply.saga)
        return await self._run(execution, reply=reply, **kwargs)

    async def _run(
        self,
        execution: SagaExecution,
        pause_on_disk: bool = False,
        raise_on_error: bool = True,
        return_execution: bool = True,
        **kwargs,
    ) -> Union[UUID, SagaExecution]:
        try:
            if pause_on_disk:
                await self._run_with_pause_on_disk(execution, **kwargs)
            else:
                await self._run_with_pause_on_memory(execution, **kwargs)
        except MinosSagaFailedExecutionException as exc:
            self.storage.store(execution)
            if raise_on_error:
                raise exc
            logger.warning(f"The execution identified by {execution.uuid!s} failed: {exc.exception!r}")

        if execution.status == SagaStatus.Finished:
            self.storage.delete(execution)

        if return_execution:
            return execution

        return execution.uuid

    async def _run_with_pause_on_disk(self, execution: SagaExecution, **kwargs) -> NoReturn:
        try:
            await execution.execute(**kwargs)
        except MinosSagaPausedExecutionStepException:
            self.storage.store(execution)
            return execution.uuid

    async def _run_with_pause_on_memory(
        self, execution: SagaExecution, reply: Optional[CommandReply] = None, **kwargs
    ) -> NoReturn:

        # noinspection PyUnresolvedReferences
        async with self.reply_pool.acquire() as handler:
            while execution.status in (SagaStatus.Created, SagaStatus.Paused):
                try:
                    await execution.execute(reply=reply, reply_topic=handler.topic, **kwargs)
                except MinosSagaPausedExecutionStepException:
                    reply = await self._get_reply(handler, execution, **kwargs)
                self.storage.store(execution)

    @staticmethod
    async def _get_reply(handler: MinosHandler, execution: SagaExecution, **kwargs) -> CommandReply:
        reply = None
        while reply is None or reply.saga != execution.uuid:
            try:
                entry = await handler.get_one(**kwargs)
            except Exception as exc:
                execution.status = SagaStatus.Errored
                raise MinosSagaFailedExecutionException(exc)
            reply = entry.data
        return reply

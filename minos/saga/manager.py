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
    MinosSagaFailedExecutionStepException,
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

    handler: MinosHandler = Provide["handler"]

    def __init__(
        self,
        storage: SagaExecutionStorage,
        definitions: dict[str, Saga],
        handler: Optional[MinosHandler] = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.storage = storage
        self.definitions = definitions

        if handler is not None:
            self.handler = handler

        if self.handler is None or isinstance(self.handler, Provide):
            raise MinosHandlerNotProvidedException("A handler instance is required.")

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
    ) -> UUID:
        if definition is None:
            definition = self.definitions.get(name)
        execution = SagaExecution.from_saga(definition, context=context)
        return await self._run(execution, **kwargs)

    async def _load_and_run(self, reply: CommandReply, **kwargs) -> UUID:
        execution = self.storage.load(reply.saga)
        return await self._run(execution, reply=reply, **kwargs)

    async def _run(self, execution: SagaExecution, asynchronous: bool = True, **kwargs) -> UUID:
        try:
            if asynchronous:
                await self._run_asynchronously(execution, asynchronous=asynchronous, **kwargs)
            else:
                await self._run_synchronously(execution, asynchronous=asynchronous, **kwargs)
        except MinosSagaFailedExecutionStepException as exc:
            logger.warning(f"The execution identified by {execution.uuid!s} failed: {exc.exception!r}")
            self.storage.store(execution)

        if execution.status == SagaStatus.Finished:
            self.storage.delete(execution)

        return execution.uuid

    async def _run_synchronously(self, execution: SagaExecution, timeout: float = 10, **kwargs) -> NoReturn:
        while execution.status in (SagaStatus.Created, SagaStatus.Paused):
            try:
                await execution.execute(**kwargs)
            except MinosSagaPausedExecutionStepException:
                entry = await self.handler.get_one(
                    [f"{execution.uuid!s}_{execution.definition_name}Reply"], timeout=timeout
                )
                kwargs["reply"] = entry.data
                self.storage.store(execution)

    async def _run_asynchronously(self, execution: SagaExecution, **kwargs) -> NoReturn:
        try:
            await execution.execute(**kwargs)
        except MinosSagaPausedExecutionStepException:
            self.storage.store(execution)
            return execution.uuid

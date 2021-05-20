"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

import logging
from uuid import (
    UUID,
)

from minos.common import (
    CommandReply,
    MinosConfig,
    MinosSagaManager,
    import_module,
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

    # noinspection PyUnusedLocal
    def __init__(self, storage: SagaExecutionStorage, definitions: dict[str, Saga], *args, **kwargs):
        self.storage = storage
        self.definitions = definitions

    @classmethod
    def from_config(cls, *args, config: MinosConfig = None, **kwargs) -> MinosSagaManager:
        """Build an instance from config.

        :param args: Additional positional arguments.
        :param config: Config instance.
        :param kwargs: Additional named arguments.
        :return: A new ``classmethod`` instance.
        """
        storage = SagaExecutionStorage.from_config(config=config, **kwargs)
        definitions = _build_definitions(config.saga.items)
        return cls(*args, storage=storage, definitions=definitions, **kwargs)

    def _run_new(self, name: str, **kwargs) -> UUID:
        definition = self.definitions.get(name)
        execution = SagaExecution.from_saga(definition)
        return self._run(execution, **kwargs)

    def _load_and_run(self, reply: CommandReply, **kwargs) -> UUID:
        execution = self.storage.load(reply.task_id)
        return self._run(execution, reply=reply, **kwargs)

    def _run(self, execution: SagaExecution, **kwargs) -> UUID:
        try:
            execution.execute(**kwargs)
        except MinosSagaPausedExecutionStepException:
            self.storage.store(execution)
            return execution.uuid
        except MinosSagaFailedExecutionStepException:
            logger.warning(f"The execution identified by {execution.uuid} failed.")
            self.storage.store(execution)
            return execution.uuid

        self.storage.delete(execution)
        return execution.uuid

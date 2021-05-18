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
    Optional,
)
from uuid import (
    UUID,
)

from minos.common import (
    CommandReply,
    MinosBaseBroker,
    MinosConfig,
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


class SagaManager(object):
    """TODO"""

    def __init__(self, storage: SagaExecutionStorage, broker: MinosBaseBroker, definitions: dict[str, Saga]):
        self.storage = storage
        self.broker = broker
        self.definitions = definitions

    @classmethod
    def from_config(cls, *args, config: MinosConfig, **kwargs) -> SagaManager:
        """TODO

        :param args: TODO
        :param config: TODO
        :param kwargs: TODO
        :return: TODO
        """
        storage = SagaExecutionStorage(**kwargs)
        broker = None

        def _fn(item) -> Saga:
            controller = import_module(item.controller)
            return getattr(controller, item.action)

        definitions = {item.name: _fn(item) for item in config.saga.items}

        return cls(storage=storage, broker=broker, definitions=definitions)

    def run(self, name: Optional[str] = None, reply: Optional[CommandReply] = None) -> UUID:
        """Perform a run of a ``Saga``.

        The run can be a new one (if a name is provided) or continue execution a previous one (if a reply is provided).

        :param name: The name of the saga to be executed.
        :param reply: The reply that relaunches a saga execution.
        :return: This method does not return anything.
        """
        if name is not None:
            return self._run_new(name)

        if reply is not None:
            return self._load_and_run(reply)

        raise ValueError("At least a 'name' or a 'reply' must be provided.")

    def _run_new(self, name: str) -> UUID:
        definition = self.definitions.get(name)
        execution = SagaExecution.from_saga(definition)
        return self._run(execution)

    def _load_and_run(self, reply: CommandReply) -> UUID:
        execution = self.storage.load(reply.task_id)
        return self._run(execution, reply=reply)

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

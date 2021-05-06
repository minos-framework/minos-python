"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import inspect
from abc import (
    ABC,
    abstractmethod,
)
from collections import (
    Callable,
)
from typing import (
    Any,
)

from ..exceptions import (
    MinosSagaException,
)
from ..storage import (
    MinosSagaStorage,
)
from .context import (
    SagaContext,
)


class Executor(ABC):
    @abstractmethod
    def exec(
        self, operation: dict[str, Any], context: SagaContext, storage: MinosSagaStorage,
    ):
        pass


class LocalExecutor(Executor):
    def __init__(self, loop):
        self._loop = loop

    def exec(
        self, operation: dict[str, Any], context: SagaContext, storage: MinosSagaStorage,
    ):
        """TODO

        :param operation: TODO
        :param context: TODO
        :param storage: TODO
        :return: TODO
        """
        storage.create_operation(operation)
        try:
            context = self._exec_function(operation["callback"], context)
        except MinosSagaException as error:
            storage.operation_error_db(operation["id"], error)
            raise error
        storage.store_operation_response(operation["id"], context)

        return context

    def _exec_function(self, func: Callable, context: SagaContext) -> SagaContext:
        """TODO

        :param func: TODO
        :param context: TODO
        :return: TODO
        """
        result = func(context)
        if inspect.isawaitable(result):
            result = self._loop.run_until_complete(result)
            return result
        return result

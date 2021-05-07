"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import inspect
from abc import (
    abstractmethod,
)
from collections import (
    Callable,
)
from typing import (
    Any,
)

from ...exceptions import (
    MinosSagaException,
)
from ...storage import (
    MinosSagaStorage,
)
from ..context import (
    SagaContext,
)


class LocalExecutor(object):
    """TODO"""

    def __init__(self, storage: MinosSagaStorage, loop):
        self.storage = storage
        self.loop = loop

    @abstractmethod
    def exec(self, operation: dict[str, Any], context: SagaContext):
        """TODO

        :param operation: TODO
        :param context: TODO
        :return: TODO
        """
        self.storage.create_operation(operation)
        try:
            context = self._exec_function(operation["callback"], context)
        except MinosSagaException as error:
            self.storage.operation_error_db(operation["id"], error)
            raise error
        self.storage.store_operation_response(operation["id"], context)

        return context

    def _exec_function(self, func: Callable, context: SagaContext) -> SagaContext:
        """TODO

        :param func: TODO
        :param context: TODO
        :return: TODO
        """
        result = func(context)
        if inspect.isawaitable(result):
            result = self.loop.run_until_complete(result)
            return result
        return result

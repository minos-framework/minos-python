"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import inspect
import uuid
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


class InvokeParticipantExecutor(Executor):
    """TODO"""

    def exec(self, operation: dict, context: SagaContext):
        """TODO

        :param operation: TODO
        :param context: TODO
        :return: TODO
        """
        if operation is None:
            return context

        self.storage.create_operation(operation)
        try:
            context = self._invoke_participant(operation["name"])
        except MinosSagaException as error:
            self.storage.operation_error_db(operation["id"], error)
            raise error
        self.storage.store_operation_response(operation["id"], context)

        if operation["callback"] is None:
            return context

        callback_operation = {
            "id": str(uuid.uuid4()),
            "type": "invokeParticipant_callback",
            "name": operation["name"],
            "callback": operation["callback"],
        }
        context = super().exec(callback_operation, context)

        return context

    @staticmethod
    def _invoke_participant(name) -> SagaContext:
        if name == "Shipping":
            raise MinosSagaException("invokeParticipantTest exception")

        # noinspection PyTypeChecker
        return "_invokeParticipant Response"


class WithCompensationExecutor(Executor):
    """TODO"""

    def exec(self, operation: dict, context: SagaContext):
        """TODO

        :param operation: TODO
        :param context: TODO
        :return: TODO
        """
        if operation is None:
            return context

        self.storage.create_operation(operation)
        try:
            context = self._with_compensation(operation["name"])
        except MinosSagaException as error:
            raise error
        self.storage.store_operation_response(operation["id"], context)

        if operation["callback"] is None:
            return context

        callback_operation = {
            "id": str(uuid.uuid4()),
            "type": "withCompensation_callback",
            "name": operation["name"],
            "callback": operation["callback"],
        }
        context = super().exec(callback_operation, context)

        return context

    # noinspection PyUnusedLocal
    @staticmethod
    def _with_compensation(name) -> SagaContext:
        # noinspection PyTypeChecker
        return "_withCompensation Response"


class OnReplyExecutor(Executor):
    """TODO"""

    def exec(self, operation: dict, context: SagaContext):
        """TODO

        :param operation: TODO
        :param context: TODO
        :return: TODO
        """
        callback_operation = {
            "id": str(uuid.uuid4()),
            "type": operation["type"],
            "name": "",
            "callback": operation["callback"],
        }
        context = super().exec(callback_operation, context)

        return context

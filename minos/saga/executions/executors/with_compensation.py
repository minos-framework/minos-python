"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import uuid
from typing import (
    Any,
)

from ...exceptions import (
    MinosSagaException,
)
from ..context import (
    SagaContext,
)
from .local import (
    LocalExecutor,
)


class WithCompensationExecutor(LocalExecutor):
    """TODO"""

    def exec(self, operation: dict[str, Any], context: SagaContext):
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

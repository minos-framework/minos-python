"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from abc import (
    abstractmethod,
)
from typing import (
    NoReturn,
)

from minos.common import (
    Aggregate,
)

from ...definitions import (
    SagaStepOperation,
)
from ...exceptions import (
    MinosSagaException,
    MinosSagaFailedExecutionStepException,
)
from ..context import (
    SagaContext,
)
from .local import (
    LocalExecutor,
)


class PublishExecutor(LocalExecutor):
    """TODO"""

    def exec(self, operation: SagaStepOperation, context: SagaContext):
        """TODO

        :param operation: TODO
        :param context: TODO
        :return: TODO
        """
        if operation is None:
            return context

        try:
            request = self._run_callback(operation, context)
            self.publish(request)
        except MinosSagaException as exc:
            raise exc
        except Exception as e:
            exc = MinosSagaFailedExecutionStepException()  # FIXME: Include explanation.
            raise exc

        return context

    @abstractmethod
    def _run_callback(self, operation: SagaStepOperation, context: SagaContext) -> Aggregate:
        raise NotImplementedError

    @staticmethod
    def publish(request: Aggregate) -> NoReturn:
        """TODO

        :param request: TODO
        :return: TODO
        """
        # TODO: Publish the command
        pass

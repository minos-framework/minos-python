"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from uuid import (
    UUID,
    uuid4,
)

from ..definitions import (
    Saga,
)
from .context import (
    SagaContext,
)
from .status import (
    SagaStatus,
)
from .step import (
    SagaStepExecution,
)


class SagaExecution(object):
    """TODO"""

    def __init__(
        self, definition: Saga, uuid: UUID, steps: [SagaStepExecution], context: SagaContext, status: SagaStatus
    ):
        self.uuid = uuid
        self.definition = definition
        self.steps = steps
        self.context = context
        self.status = status

    @classmethod
    def from_saga(cls, definition: Saga):
        """TODO

        :param definition: TODO
        :return: TODO
        """
        return cls(definition, uuid=uuid4(), steps=list(), context=SagaContext(), status=SagaStatus.Created)

    def execute(self):
        """TODO

        :return: TODO
        """
        pass

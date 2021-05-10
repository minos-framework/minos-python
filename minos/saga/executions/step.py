"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

from typing import (
    Any,
    NoReturn,
    Optional,
)

from ..definitions import (
    SagaStep,
)
from ..exceptions import (
    MinosSagaException,
    MinosSagaFailedExecutionStepException,
    MinosSagaPausedExecutionStepException,
)
from .context import (
    SagaContext,
)
from .executors import (
    InvokeParticipantExecutor,
    OnReplyExecutor,
    WithCompensationExecutor,
)
from .status import (
    SagaStepStatus,
)


class SagaExecutionStep(object):
    """TODO"""

    def __init__(self, definition: SagaStep, status: SagaStepStatus = SagaStepStatus.Created):
        self.definition = definition
        self.status = status
        self.already_rollback = False

    def execute(self, context: SagaContext, response: Optional[Any] = None, *args, **kwargs) -> SagaContext:
        """TODO

        :param context: TODO
        :param response: TODO
        :return: TODO
        """

        self._execute_invoke_participant(context, *args, **kwargs)
        context = self._execute_on_reply(context, response, *args, **kwargs)

        self.status = SagaStepStatus.Finished
        return context

    def _execute_invoke_participant(self, context: SagaContext, *args, **kwargs) -> NoReturn:
        if self.status != SagaStepStatus.Created:
            return

        self.status = SagaStepStatus.RunningInvokeParticipant
        executor = InvokeParticipantExecutor(*args, **kwargs)
        try:
            executor.exec(self.definition.raw_invoke_participant, context)
        except MinosSagaException:
            self.status = SagaStepStatus.ErroredInvokeParticipant
            self.rollback(context, *args, **kwargs)
            raise MinosSagaFailedExecutionStepException()
        self.status = SagaStepStatus.FinishedInvokeParticipant

    def _execute_on_reply(self, context: SagaContext, response: Optional[Any] = None, *args, **kwargs) -> SagaContext:
        self.status = SagaStepStatus.RunningOnReply
        executor = OnReplyExecutor(*args, **kwargs)
        # noinspection PyBroadException
        try:
            context = executor.exec(self.definition.raw_on_reply, context, response=response)
        except MinosSagaPausedExecutionStepException as exc:
            self.status = SagaStepStatus.PausedOnReply
            raise exc
        except Exception as e:
            self.status = SagaStepStatus.ErroredOnReply
            self.rollback(context, *args, **kwargs)
            raise MinosSagaFailedExecutionStepException()
        return context

    def rollback(self, context: SagaContext, *args, **kwargs) -> SagaContext:
        """TODO

        :param context: TODO
        :return: TODO
        """
        if self.already_rollback:
            return context

        executor = WithCompensationExecutor(*args, **kwargs)
        executor.exec(self.definition.raw_with_compensation, context)

        self.already_rollback = True
        return context

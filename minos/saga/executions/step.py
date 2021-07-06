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
    Iterable,
    NoReturn,
    Optional,
    Union,
)

from minos.common import (
    CommandReply,
)

from ..definitions import (
    SagaStep,
)
from ..exceptions import (
    MinosCommandReplyFailedException,
    MinosSagaFailedExecutionStepException,
    MinosSagaPausedExecutionStepException,
    MinosSagaRollbackExecutionStepException,
)
from .context import (
    SagaContext,
)
from .executors import (
    OnReplyExecutor,
    PublishExecutor,
)
from .status import (
    SagaStepStatus,
)


class SagaExecutionStep(object):
    """Saga Execution Step class."""

    def __init__(
        self, definition: SagaStep, status: SagaStepStatus = SagaStepStatus.Created, already_rollback: bool = False,
    ):

        self.definition = definition
        self.status = status
        self.already_rollback = already_rollback

    @classmethod
    def from_raw(cls, raw: Union[dict[str, Any], SagaExecutionStep], **kwargs) -> SagaExecutionStep:
        """Build a new instance from a raw representation.

        :param raw: The raw representation of the instance.
        :param kwargs: Additional named arguments.
        :return: A ``SagaExecutionStep`` instance.
        """
        if isinstance(raw, cls):
            return raw

        current = raw | kwargs
        current["definition"] = SagaStep.from_raw(current["definition"])
        current["status"] = SagaStepStatus.from_raw(current["status"])
        return cls(**current)

    async def execute(self, context: SagaContext, reply: Optional[CommandReply] = None, *args, **kwargs) -> SagaContext:
        """Execution the step.

        :param context: The execution context to be used during the execution.
        :param reply: An optional command reply instance (to be consumed by the on_reply method).
        :return: The updated context.
        """

        await self._execute_invoke_participant(context, *args, **kwargs)
        context = await self._execute_on_reply(context, reply, *args, **kwargs)

        self.status = SagaStepStatus.Finished
        return context

    async def _execute_invoke_participant(self, context: SagaContext, *args, **kwargs) -> NoReturn:
        if self.status != SagaStepStatus.Created:
            return

        self.status = SagaStepStatus.RunningInvokeParticipant
        executor = PublishExecutor(*args, **kwargs)
        try:
            await executor.exec(self.definition.invoke_participant_operation, context)
        except MinosSagaFailedExecutionStepException as exc:
            self.status = SagaStepStatus.ErroredInvokeParticipant
            raise exc
        self.status = SagaStepStatus.FinishedInvokeParticipant

    async def _execute_on_reply(
        self, context: SagaContext, reply: Optional[CommandReply] = None, *args, **kwargs
    ) -> SagaContext:
        self.status = SagaStepStatus.RunningOnReply
        executor = OnReplyExecutor(*args, **kwargs)

        try:
            context = await executor.exec(self.definition.on_reply_operation, context, reply)
        except MinosSagaPausedExecutionStepException as exc:
            self.status = SagaStepStatus.PausedOnReply
            raise exc
        except MinosCommandReplyFailedException as exc:
            self.status = SagaStepStatus.ErroredOnReply
            raise MinosSagaFailedExecutionStepException(exc)
        except MinosSagaFailedExecutionStepException as exc:
            self.status = SagaStepStatus.ErroredOnReply
            await self.rollback(context, *args, **kwargs)
            raise exc

        return context

    async def rollback(self, context: SagaContext, *args, **kwargs) -> SagaContext:
        """Revert the invoke participant operation with a with compensation operation.

        :param context: Execution context.
        :return: The updated execution context.
        """
        if self.status == SagaStepStatus.Created:
            raise MinosSagaRollbackExecutionStepException("There is nothing to rollback.")

        if self.already_rollback:
            raise MinosSagaRollbackExecutionStepException("The step was already rollbacked.")

        executor = PublishExecutor(*args, **kwargs)
        await executor.exec(self.definition.with_compensation_operation, context)

        self.already_rollback = True
        return context

    @property
    def raw(self) -> dict[str, Any]:
        """Compute a raw representation of the instance.

        :return: A ``dict`` instance.
        """
        return {
            "definition": self.definition.raw,
            "status": self.status.raw,
            "already_rollback": self.already_rollback,
        }

    def __eq__(self, other: SagaStep) -> bool:
        return type(self) == type(other) and tuple(self) == tuple(other)

    def __iter__(self) -> Iterable:
        yield from (
            self.definition,
            self.status,
            self.already_rollback,
        )

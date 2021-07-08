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
    Any,
    Iterable,
    NoReturn,
    Optional,
    Union,
)
from uuid import (
    UUID,
)

from minos.common import (
    CommandReply,
)

from ..definitions import (
    Saga,
    SagaStep,
)
from ..exceptions import (
    MinosSagaExecutionAlreadyExecutedException,
    MinosSagaExecutionStepException,
    MinosSagaExecutorException,
    MinosSagaFailedCommitCallbackException,
    MinosSagaFailedExecutionStepException,
    MinosSagaNotCommittedException,
    MinosSagaPausedExecutionStepException,
    MinosSagaRollbackExecutionException,
)
from .context import (
    SagaContext,
)
from .executors import (
    LocalExecutor,
)
from .status import (
    SagaStatus,
)
from .step import (
    SagaExecutionStep,
)

logger = logging.getLogger(__name__)


class SagaExecution(object):
    """Saga Execution class."""

    # noinspection PyUnusedLocal
    def __init__(
        self,
        definition: Saga,
        uuid: UUID,
        context: SagaContext,
        status: SagaStatus = SagaStatus.Created,
        steps: list[SagaExecutionStep] = None,
        paused_step: SagaExecutionStep = None,
        already_rollback: bool = False,
        *args,
        **kwargs,
    ):
        if not definition.committed:
            raise MinosSagaNotCommittedException("The definition must be committed before executing it.")

        if steps is None:
            steps = list()

        self.uuid = uuid
        self.definition = definition
        self.executed_steps = steps
        self.context = context
        self.status = status
        self.already_rollback = already_rollback
        self.paused_step = paused_step

    @classmethod
    def from_raw(cls, raw: Union[dict[str, Any], SagaExecution], **kwargs) -> SagaExecution:
        """Build a new instance from a raw representation.

        :param raw: The raw representation of the instance.
        :param kwargs: Additional named arguments.
        :return: A ``SagaExecution`` instance.
        """
        if isinstance(raw, cls):
            return raw

        current = raw | kwargs
        current["definition"] = Saga.from_raw(current["definition"])
        current["status"] = SagaStatus.from_raw(current["status"])
        current["context"] = SagaContext.from_avro_str(current["context"])
        current["paused_step"] = (
            None if current["paused_step"] is None else SagaExecutionStep.from_raw(current["paused_step"])
        )

        if isinstance(current["uuid"], str):
            current["uuid"] = UUID(current["uuid"])

        instance = cls(**current)

        executed_steps = (
            SagaExecutionStep.from_raw(executed_step, definition=step)
            for step, executed_step in zip(instance.definition.steps, raw.pop("executed_steps"))
        )
        for executed_step in executed_steps:
            instance._add_executed(executed_step)

        return instance

    @classmethod
    def from_saga(cls, definition: Saga, context: Optional[SagaContext] = None, *args, **kwargs) -> SagaExecution:
        """Build a new instance from a ``Saga`` object.

        :param definition: The definition of the saga.
        :param context: Initial saga execution context. If not provided, then a new empty context is created.
        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: A new ``SagaExecution`` instance.
        """
        from uuid import (
            uuid4,
        )

        if context is None:
            context = SagaContext()

        return cls(definition, uuid4(), context, *args, **kwargs)

    async def execute(self, reply: Optional[CommandReply] = None, *args, **kwargs) -> SagaContext:
        """Execute the ``Saga`` definition.

        :param reply: An optional ``CommandReply`` to be consumed by the immediately next executed step.
        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: A ``SagaContext instance.
        """
        if self.status == SagaStatus.Finished:
            raise MinosSagaExecutionAlreadyExecutedException(
                f"The {self.uuid!s} execution cannot be executed because is in {self.status!r} status."
            )
        if self.status == SagaStatus.Errored:
            if reply is None or reply.saga != self.uuid:
                raise MinosSagaExecutionAlreadyExecutedException(
                    f"The {self.uuid!s} execution cannot be executed because is in {self.status!r} status."
                )

            logger.info(f"Received 'with_compensation' reply: {reply!s}")
            return self.context

        self.status = SagaStatus.Running

        if self.paused_step is not None:
            try:
                await self._execute_one(self.paused_step, reply=reply, *args, **kwargs)
            finally:
                self.paused_step = None

        for step in self._pending_steps:
            execution_step = SagaExecutionStep(step)
            await self._execute_one(execution_step, *args, **kwargs)

        await self._execute_commit_callback(*args, **kwargs)
        self.status = SagaStatus.Finished
        return self.context

    async def _execute_one(self, execution_step: SagaExecutionStep, *args, **kwargs) -> NoReturn:
        try:
            self.context = await execution_step.execute(
                self.context, definition_name=self.definition_name, execution_uuid=self.uuid, *args, **kwargs
            )
            self._add_executed(execution_step)
        except MinosSagaFailedExecutionStepException as exc:
            await self.rollback(*args, **kwargs)
            self.status = SagaStatus.Errored
            raise exc
        except MinosSagaPausedExecutionStepException as exc:
            self.paused_step = execution_step
            self.status = SagaStatus.Paused
            raise exc

    async def _execute_commit_callback(self, *args, **kwargs) -> NoReturn:
        try:
            executor = LocalExecutor()
            self.context = await executor.exec_function(self.definition.commit_callback, self.context)
        except MinosSagaExecutorException as exc:
            await self.rollback(*args, **kwargs)
            self.status = SagaStatus.Errored
            raise MinosSagaFailedCommitCallbackException(exc.exception)

    async def rollback(self, *args, **kwargs) -> NoReturn:
        """Revert the invoke participant operation with a with compensation operation.

        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: The updated execution context.
        """

        if self.already_rollback:
            raise MinosSagaRollbackExecutionException("The saga was already rollbacked.")

        raised_exception = False
        for execution_step in reversed(self.executed_steps):
            try:
                self.context = await execution_step.rollback(
                    self.context, definition_name=self.definition_name, execution_uuid=self.uuid, *args, **kwargs
                )
            except MinosSagaExecutionStepException as exc:
                logger.warning(f"There was an exception on {type(execution_step).__name__!r} rollback: {exc!r}")
                raised_exception = True

        if raised_exception:
            raise MinosSagaRollbackExecutionException("Some execution steps failed to rollback.")

        self.already_rollback = True

    @property
    def _pending_steps(self) -> list[SagaStep]:
        offset = len(self.executed_steps)
        return self.definition.steps[offset:]

    def _add_executed(self, executed_step: SagaExecutionStep) -> NoReturn:
        self.executed_steps.append(executed_step)

    @property
    def raw(self) -> dict[str, Any]:
        """Compute a raw representation of the instance.

        :return: A ``dict`` instance.
        """
        return {
            "definition": self.definition.raw,
            "uuid": str(self.uuid),
            "status": self.status.raw,
            "executed_steps": [step.raw for step in self.executed_steps],
            "paused_step": None if self.paused_step is None else self.paused_step.raw,
            "context": self.context.avro_str,
            "already_rollback": self.already_rollback,
        }

    @property
    def definition_name(self) -> str:
        """Get the ``Saga``` Definition name.

        :return: An ``str`` instance.
        """
        return self.definition.name

    def __eq__(self, other: SagaStep) -> bool:
        return type(self) == type(other) and tuple(self) == tuple(other)

    def __iter__(self) -> Iterable:
        yield from (
            self.definition,
            self.uuid,
            self.status,
            self.executed_steps,
            self.context,
            self.already_rollback,
        )

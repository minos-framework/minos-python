"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

from uuid import (
    UUID,
)

from minos.common import (
    AggregateDiff,
    Model,
    ModelRefExtractor,
    ModelRefInjector,
    ModelType,
)
from minos.saga import (
    Saga,
    SagaContext,
    SagaExecution,
)

from .abc import (
    Service,
)


class QueryService(Service):
    """Query Service class"""

    async def _pre_event_handle(self, diff: AggregateDiff) -> AggregateDiff:
        definition = self._build_event_saga(diff)

        execution = SagaExecution.from_saga(definition)
        await self.saga_manager._run_with_pause_on_memory(execution)
        context = execution.context

        return context["diff"]

    def _build_event_saga(self, diff: AggregateDiff) -> Saga:
        missing = ModelRefExtractor(diff.fields_diff).build()

        saga = Saga("")
        for name, uuids in missing.items():
            saga = (
                saga.step()
                .invoke_participant(f"Get{name}s", self._invoke_callback, SagaContext(uuids=list(uuids)))
                .on_reply(f"{name}s")
            )
        saga = saga.commit(self._build_commit_callback, parameters=SagaContext(diff=diff))

        return saga

    # noinspection PyUnusedLocal
    @staticmethod
    def _invoke_callback(context: SagaContext, uuids: list[UUID]):
        return ModelType.build("Query", {"uuids": list[UUID]})(uuids=uuids)

    @classmethod
    def _build_commit_callback(cls, context: SagaContext, diff: AggregateDiff) -> SagaContext:
        recovered = cls._build_recovered(context)
        diff = ModelRefInjector(diff, recovered).build()
        return SagaContext(diff=diff)

    @staticmethod
    def _build_recovered(context: SagaContext) -> dict[UUID, Model]:
        recovered = dict()
        for k in context.keys():
            for v in context[k]:
                recovered[v.uuid] = v
        return recovered

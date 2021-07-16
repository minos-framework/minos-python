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
    classname,
    import_module,
)
from minos.saga import (
    Saga,
    SagaContext,
)


def _build_event_saga(diff, controller, action) -> Saga:
    missing = _get_missing(diff)
    saga = Saga("ordersQuery")

    for name, uuids in missing.items():
        saga = (
            saga
            .step()
            .invoke_participant(f"Get{name}s", _invoke_callback, SagaContext(uuids=list(uuids)))
            .on_reply(f"{name}s")
        )

    saga = saga.commit(
        _build_commit_callback,
        parameters=SagaContext(diff=diff, controller=classname(type(controller)), action=action.__name__)
    )

    return saga


def _get_missing(diff: AggregateDiff) -> dict[str, set[UUID]]:
    return ModelRefExtractor(diff.fields_diff).build()


# noinspection PyUnusedLocal
def _invoke_callback(context: SagaContext, uuids: list[UUID]):
    return ModelType.build("Query", {"uuids": list[UUID]})(uuids=uuids)


async def _build_commit_callback(
    context: SagaContext, diff: AggregateDiff, controller: str, action: str
) -> SagaContext:
    recovered = _build_recovered(context)
    diff = _put_missing(diff, recovered)

    controller = import_module(controller)()
    fn = getattr(controller, action)
    await fn(diff)
    return context


def _build_recovered(context: SagaContext) -> dict[UUID, Model]:
    recovered = dict()
    for k in context.keys():
        if k in ("diff", "controller", "action"):
            continue
        for v in context[k]:
            recovered[v.uuid] = v
    return recovered


def _put_missing(diff: AggregateDiff, recovered: dict[UUID, Model]) -> AggregateDiff:
    return ModelRefInjector(diff, recovered).build()

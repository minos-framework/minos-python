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
    SagaExecution,
)


def _build_event_saga_execution(diff, controller, action):
    missing = _get_missing(diff)
    definition = _build_saga(missing)
    execution = SagaExecution.from_saga(
        definition, context=SagaContext(diff=diff, controller=classname(type(controller)), action=action.__name__)
    )
    return execution


def _get_missing(diff: AggregateDiff) -> dict[str, set[UUID]]:
    return ModelRefExtractor(diff.fields_diff).build()


def _build_saga(missing: dict[str, set[UUID]]) -> Saga:
    saga = Saga("ordersQuery")

    for name, uuids in missing.items():
        saga = saga.step().invoke_participant(f"Get{name}s", _invoke_callback, uuids=list(uuids)).on_reply(f"{name}s")

    saga = saga.commit(_build_commit_callback)

    return saga


# noinspection PyUnusedLocal
def _invoke_callback(context: SagaContext, uuids: list[UUID]):
    return ModelType.build("Query", {"uuids": list[UUID]})(uuids=uuids)


async def _build_commit_callback(context: SagaContext) -> SagaContext:
    diff = context["diff"]
    recovered = _build_recovered(context)
    diff = _put_missing(diff, recovered)

    controller = import_module(context["controller"])
    controller = controller()
    fn = getattr(controller, context["action"])
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

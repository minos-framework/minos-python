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
    MinosSagaManager,
    ModelRefExtractor,
    ModelRefInjector,
    ModelType,
)
from minos.saga import (
    Saga,
    SagaContext,
    SagaExecution,
    SagaStatus,
)

from .exceptions import (
    MinosQueryServiceException,
)


class PreEventHandler:
    """TODO"""

    @classmethod
    async def handle(cls, saga_manager: MinosSagaManager, diff: AggregateDiff) -> AggregateDiff:
        """TODO

        :param saga_manager:
        :param diff:
        :return:
        """
        definition = cls.build_saga(diff)
        execution = await saga_manager.run(
            definition=definition, pause_on_disk=False, return_execution=True, raise_on_error=True
        )
        if not isinstance(execution, SagaExecution) or execution.status != SagaStatus.Finished:
            raise MinosQueryServiceException("The saga execution could not finish.")
        return execution.context["diff"]

    @classmethod
    def build_saga(cls, diff: AggregateDiff) -> Saga:
        """TODO

        :param diff: TODO
        :return: TODO
        """
        missing = ModelRefExtractor(diff.fields_diff).build()

        saga = Saga("")
        for name, uuids in missing.items():
            saga = (
                saga.step()
                .invoke_participant(f"Get{name}s", cls.invoke_callback, SagaContext(uuids=list(uuids)))
                .on_reply(f"{name}s")
            )
        saga = saga.commit(cls.commit_callback, parameters=SagaContext(diff=diff))

        return saga

    # noinspection PyUnusedLocal
    @staticmethod
    def invoke_callback(context: SagaContext, uuids: list[UUID]):
        """TODO

        :param context: TODO
        :param uuids: TODO
        :return: TODO
        """
        return ModelType.build("Query", {"uuids": list[UUID]})(uuids=uuids)

    @classmethod
    def commit_callback(cls, context: SagaContext, diff: AggregateDiff) -> SagaContext:
        """TODO

        :param context:TODO
        :param diff:TODO
        :return:TODO
        """

        recovered = dict()
        for k in context.keys():
            for v in context[k]:
                recovered[v.uuid] = v

        diff = ModelRefInjector(diff, recovered).build()
        return SagaContext(diff=diff)

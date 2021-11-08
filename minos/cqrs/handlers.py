from __future__ import (
    annotations,
)

from typing import (
    Optional,
    TypeVar,
)
from uuid import (
    UUID,
)

from minos.aggregate import (
    AggregateDiff,
    ModelRefExtractor,
    ModelRefInjector,
)
from minos.saga import (
    RemoteSagaStep,
    Saga,
    SagaContext,
    SagaExecution,
    SagaManager,
    SagaOperation,
    SagaRequest,
    SagaResponse,
    SagaStatus,
)

from .exceptions import (
    MinosNotAnyMissingReferenceException,
    MinosQueryServiceException,
)


class PreEventHandler:
    """Pre Event Handler class."""

    @classmethod
    async def handle(
        cls, diff: T, saga_manager: SagaManager, user: Optional[UUID] = None, resolve_references: bool = True
    ) -> T:
        """Handle pre event function.

        :param diff: The initial aggregate difference.
        :param user: TODO
        :param saga_manager: The saga manager to be used to compute the queries to another microservices.
        :param resolve_references: If ``True``, reference are resolved, otherwise, the ``AggregateDiff`` is returned
            without any modification.
        :return: The recomposed aggregate difference.
        """
        if not isinstance(diff, AggregateDiff) or not resolve_references:
            return diff

        try:
            definition = cls.build_saga(diff)
        except MinosNotAnyMissingReferenceException:
            return diff

        execution = await saga_manager.run(definition=definition, user=user)
        if not isinstance(execution, SagaExecution) or execution.status != SagaStatus.Finished:
            raise MinosQueryServiceException("The saga execution could not finish.")
        return execution.context["diff"]

    @classmethod
    def build_saga(cls, diff: AggregateDiff) -> Saga:
        """Build a saga to query the referenced aggregates.

        :param diff: The base aggregate difference.
        :return: A saga instance.
        """
        missing = ModelRefExtractor(diff.fields_diff).build()

        if not len(missing):
            raise MinosNotAnyMissingReferenceException("The diff does not have any missing reference.")

        steps = [
            RemoteSagaStep(
                on_execute=SagaOperation(cls.on_execute, name=name, uuids=list(uuids)),
                on_success=SagaOperation(cls.on_success, name=name),
            )
            for name, uuids in missing.items()
        ]
        commit = SagaOperation(cls.commit, diff=diff)

        saga = Saga(steps=steps, commit=commit)

        return saga

    # noinspection PyUnusedLocal
    @staticmethod
    def on_execute(context: SagaContext, name: str, uuids: list[UUID]) -> SagaRequest:
        """Callback to prepare data before invoking participants.

        :param context: The saga context (ignored).
        :param name: The name of the aggregate.
        :param uuids: The list of identifiers.
        :return: A dto instance.
        """
        return SagaRequest(f"Get{name}s", {"uuids": uuids})

    @staticmethod
    async def on_success(context: SagaContext, response: SagaResponse, name: str) -> SagaContext:
        """Callback to handle the response on successful cases.

        :param context: The previous saga context instance.
        :param response: The obtained response.
        :param name: The name of the aggregate.
        :return: The new saga context instance.
        """
        context[f"{name.lower()}s"] = await response.content()
        return context

    @classmethod
    def commit(cls, context: SagaContext, diff: AggregateDiff) -> SagaContext:
        """Callback to be executed at the end of the saga.

        :param context: The saga execution context.
        :param diff: The initial aggregate difference.
        :return: A saga context containing the enriched aggregate difference.
        """

        recovered = dict()
        for k in context.keys():
            for v in context[k]:
                recovered[v.uuid] = v

        diff = ModelRefInjector(diff, recovered).build()
        return SagaContext(diff=diff)


T = TypeVar("T")

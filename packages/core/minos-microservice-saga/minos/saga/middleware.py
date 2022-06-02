from collections.abc import (
    Awaitable,
    Callable,
)
from typing import (
    Optional,
)
from uuid import (
    UUID,
)

from minos.networks import (
    REQUEST_HEADERS_CONTEXT_VAR,
    BrokerRequest,
    Request,
    Response,
)
from minos.transactions import (
    TransactionEntry,
)

from .utils import (
    get_service_name,
)


async def transactional_command(
    request: Request, inner: Callable[[Request], Awaitable[Optional[Response]]]
) -> Optional[Response]:
    """Execute the command transactionally if it comes from a saga.

    :param request: The request containing the data.
    :param inner: The inner handling function to be executed.
    :return: The response generated by the inner handling function.
    """
    try:
        if isinstance(request, BrokerRequest):
            message = request.raw
            if raw_transaction_uuids := message.headers.get("transactions"):
                transaction_uuids = list(map(UUID, raw_transaction_uuids.split(",")))
                return await _transaction(request, inner, transaction_uuids)
        return await inner(request)
    finally:
        if (headers := REQUEST_HEADERS_CONTEXT_VAR.get()) is not None:
            if (raw_transaction_uuids := headers.get("transactions")) is not None:
                raw_transaction_uuids_parts = raw_transaction_uuids.rsplit(",", 1)
                if len(raw_transaction_uuids_parts) == 1:
                    del headers["transactions"]
                else:
                    headers["transactions"] = raw_transaction_uuids_parts[0]

            related_services = set()
            if raw_related_services := headers.get("related_services"):
                related_services.update(raw_related_services.split(","))
            related_services.add(get_service_name())
            headers["related_services"] = ",".join(related_services)


async def _transaction(
    request: Request, inner: Callable[[Request], Awaitable[Optional[Response]]], transaction_uuids: list[UUID]
) -> Optional[Response]:
    if len(transaction_uuids):
        async with TransactionEntry(uuid=transaction_uuids[0], autocommit=False):
            return await _transaction(request, inner, transaction_uuids[1:])

    return await inner(request)

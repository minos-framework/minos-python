"""
Copyright (C) 2021 Clariteia SL
This file is part of minos framework.
Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from abc import (
    ABC,
)
from functools import (
    partial,
)
from inspect import (
    getmembers,
    isfunction,
    ismethod,
)
from typing import (
    Optional,
    Type,
)
from uuid import (
    UUID,
)

from cached_property import (
    cached_property,
)
from dependency_injector.wiring import (
    Provide,
)

from minos.common import (
    Aggregate,
    MinosConfig,
    MinosSagaManager,
    ModelType,
    import_module,
)
from minos.networks import (
    EnrouteDecorator,
    Request,
    Response,
    ResponseException,
    WrappedRequest,
    enroute,
)

from .exceptions import (
    MinosIllegalHandlingException,
)
from .handlers import (
    PreEventHandler,
)


class Service(ABC):
    """Base Service class"""

    config: MinosConfig = Provide["config"]
    saga_manager: MinosSagaManager = Provide["saga_manager"]

    def __init__(
        self, *args, config: Optional[MinosConfig] = None, saga_manager: Optional[MinosSagaManager] = None, **kwargs,
    ):
        if config is not None:
            self.config = config
        if saga_manager is not None:
            self.saga_manager = saga_manager

    @classmethod
    def __get_enroute__(cls, config: MinosConfig) -> dict[str, set[EnrouteDecorator]]:
        result = dict()
        for name, fn in getmembers(cls, predicate=lambda x: ismethod(x) or isfunction(x)):
            if not hasattr(fn, "__decorators__"):
                continue
            result[name] = fn.__decorators__
        return result


class CommandService(Service, ABC):
    """Command Service class"""

    @staticmethod
    def _pre_command_handle(request: Request) -> Request:
        return request

    @staticmethod
    def _pre_query_handle(request: Request) -> Request:
        raise MinosIllegalHandlingException("Queries cannot be handled by `CommandService` inherited classes.")

    @staticmethod
    def _pre_event_handle(request: Request) -> Request:
        raise MinosIllegalHandlingException("Events cannot be handled by `CommandService` inherited classes.")


class QueryService(Service, ABC):
    """Query Service class"""

    @staticmethod
    def _pre_command_handle(request: Request) -> Request:
        raise MinosIllegalHandlingException("Commands cannot be handled by `QueryService` inherited classes.")

    @staticmethod
    def _pre_query_handle(request: Request) -> Request:
        return request

    def _pre_event_handle(self, request: Request) -> Request:
        fn = partial(PreEventHandler.handle, saga_manager=self.saga_manager)
        return WrappedRequest(request, fn)

    @classmethod
    def __get_enroute__(cls, config: MinosConfig) -> dict[str, set[EnrouteDecorator]]:
        aggregate_name = config.service.aggregate.rsplit(".", 1)[-1]
        additional = {
            cls.__get_aggregate__.__name__: {enroute.broker.query(f"Get{aggregate_name}")},
            cls.__get_aggregates__.__name__: {enroute.broker.query(f"Get{aggregate_name}s")},
        }
        return super().__get_enroute__(config) | additional

    async def __get_aggregate__(self, request: Request) -> Response:
        """Get product.

        :param request: The ``Request`` instance that contains the product identifier.
        :return: A ``Response`` instance containing the requested product.
        """
        try:
            content = await request.content(model_type=ModelType.build("Query", {"uuid": UUID}))
        except Exception as exc:
            raise ResponseException(f"There was a problem while parsing the given request: {exc!r}")

        try:
            product = await self.__aggregate_cls__.get_one(content["uuid"])
        except Exception as exc:
            raise ResponseException(f"There was a problem while getting the product: {exc!r}")

        return Response(product)

    async def __get_aggregates__(self, request: Request) -> Response:
        """Get products.

        :param request: The ``Request`` instance that contains the product identifiers.
        :return: A ``Response`` instance containing the requested products.
        """
        try:
            content = await request.content(model_type=ModelType.build("Query", {"uuids": list[UUID]}))
        except Exception as exc:
            raise ResponseException(f"There was a problem while parsing the given request: {exc!r}")

        try:
            iterable = self.__aggregate_cls__.get(uuids=content["uuids"])
            values = {v.uuid: v async for v in iterable}
            products = [values[uuid] for uuid in content["uuids"]]
        except Exception as exc:
            raise ResponseException(f"There was a problem while getting products: {exc!r}")

        return Response(products)

    @cached_property
    def __aggregate_cls__(self) -> Type[Aggregate]:
        # noinspection PyTypeChecker
        return import_module(self.config.service.aggregate)

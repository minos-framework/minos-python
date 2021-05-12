"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from minos.common import (
    MinosModel,
)
from minos.saga import (
    SagaContext,
)
from tests.utils import (
    Foo,
)


def create_ticket_on_reply_callback(context: SagaContext) -> MinosModel:
    treated_response = "async create_ticket_on_reply_callback response!!!!"
    return Foo(treated_response)


async def create_order_callback(context: SagaContext) -> MinosModel:
    treated_response = "create_order_callback response!!!!"
    return Foo(treated_response)


def create_ticket_callback(context: SagaContext) -> MinosModel:
    treated_response = "create_ticket_callback response!!!!"
    return Foo(treated_response)


async def create_order_callback2(context: SagaContext) -> MinosModel:
    treated_response = "create_order_callback response!!!!"
    return Foo(treated_response)


async def delete_order_callback(context: SagaContext) -> MinosModel:
    treated_response = "async delete_order_callback response!!!!"
    return Foo(treated_response)


def shipping_callback(context: SagaContext) -> MinosModel:
    treated_response = "async shipping_callback response!!!!"
    return Foo(treated_response)


async def a_callback(context: SagaContext) -> MinosModel:
    treated_response = "create_order_callback response!!!!"
    return Foo(treated_response)


async def b_callback(context: SagaContext) -> MinosModel:
    treated_response = "create_order_callback response!!!!"
    return Foo(treated_response)


async def c_callback(context: SagaContext) -> MinosModel:
    treated_response = "async create_ticket_on_reply_callback response!!!!"
    return Foo(treated_response)


def d_callback(context: SagaContext) -> MinosModel:
    treated_response = "create_order_callback response!!!!"
    return Foo(treated_response)


def e_callback(context: SagaContext) -> MinosModel:
    treated_response = "create_order_callback response!!!!"
    return Foo(treated_response)


def f_callback(context: SagaContext) -> MinosModel:
    treated_response = "async create_ticket_on_reply_callback response!!!!"
    return Foo(treated_response)

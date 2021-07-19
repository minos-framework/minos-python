"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from collections.abc import (
    Callable,
)
from typing import (
    Any,
    Awaitable,
    Union,
)

from ..context import (
    SagaContext,
)

PublishCallBack = Callable[[SagaContext, ...], Union[Any, Awaitable[Any]]]
ReplyCallBack = Callable[[Any, ...], Union[Any, Awaitable[Any]]]
CommitCallback = Callable[[SagaContext, ...], Union[None, SagaContext, Awaitable[SagaContext]]]

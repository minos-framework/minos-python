# Copyright (C) 2020 Clariteia SL
#
# This file is part of minos framework.
#
# Minos framework can not be copied and/or distributed without the express
# permission of Clariteia SL.

import functools

from minos.common import Event
from typing import Callable
from inspect import isawaitable


class Subscribe(object):
    """Event Handler class."""

    def __init__(self, func: Callable):
        functools.update_wrapper(self, func)
        self.func = func

    async def __call__(self, *args, **kwargs):
        if len(args) > 0 and isinstance(args[0], Event):
            return await self.func._handle(args[0].data, self.func)

        call = self.func(self.func, *args, **kwargs)

        if isawaitable(call):
            return await call
        return call


"""
def subscribe(func: Callable):
    async def wrapper(self, *args, **kwargs) -> NoReturn:
        if len(args) > 0 and isinstance(args[0], Event):
            return await self._handle(args[0].data, func)

        call = func(self, *args, **kwargs)

        if isawaitable(call):
            return await call
        return call

    return wrapper
"""

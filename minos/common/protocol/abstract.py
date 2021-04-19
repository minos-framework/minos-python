"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

import abc
import typing as t


class MinosBinaryProtocol(abc.ABC):
    @classmethod
    @abc.abstractmethod
    def encode(cls, headers: t.Dict, body: t.Any) -> bytes:
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def decode(cls, data: bytes):
        raise NotImplementedError

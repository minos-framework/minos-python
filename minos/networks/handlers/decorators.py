# Copyright (C) 2020 Clariteia SL
#
# This file is part of minos framework.
#
# Minos framework can not be copied and/or distributed without the express
# permission of Clariteia SL.


class BrokerCommandEnroute:
    """Broker Command Enroute class"""

    _topics: list[str]

    def __init__(self, *args, **kwargs):
        self._topics = kwargs["topics"]

    def __call__(self, fn, *args, **kwargs):
        return fn


class BrokerQueryEnroute:
    """Broker Query Enroute class"""

    _topics: list[str]

    def __init__(self, *args, **kwargs):
        self._topics = kwargs["topics"]

    def __call__(self, fn, *args, **kwargs):
        return fn


class BrokerEventEnroute:
    """Broker Event Enroute class"""

    _topics: list[str]

    def __init__(self, *args, **kwargs):
        self._topics = kwargs["topics"]

    def __call__(self, fn, *args, **kwargs):
        return fn


class BrokerEnroute:
    """Broker Enroute class"""

    command = BrokerCommandEnroute
    query = BrokerQueryEnroute
    event = BrokerEventEnroute


class RestCommandEnroute:
    """Rest Command Enroute class"""

    _topics: list[str]

    def __init__(self, *args, **kwargs):
        self._topics = kwargs["topics"]

    def __call__(self, fn, *args, **kwargs):
        return fn


class RestQueryEnroute:
    """Rest Query Enroute class"""

    _url: str
    _method: str

    def __init__(self, *args, **kwargs):
        self._url = kwargs["url"]
        self._method = kwargs["method"]

    def __call__(self, fn, *args, **kwargs):
        return fn


class RestEnroute:
    """Rest Enroute class"""

    command = RestCommandEnroute
    query = RestQueryEnroute


class Enroute:
    """Enroute decorator main class"""

    broker = BrokerEnroute
    rest = RestEnroute

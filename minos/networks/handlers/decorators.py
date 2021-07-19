# Copyright (C) 2020 Clariteia SL
#
# This file is part of minos framework.
#
# Minos framework can not be copied and/or distributed without the express
# permission of Clariteia SL.


class BrokerCommandEnroute:
    """Broker Command Enroute class"""

    def __init__(self, topics: list[str], **kwargs):
        self.kwargs = kwargs
        self.topics = topics

    def __call__(self, fn):
        def wrapper(*args, analyze_mode: bool = False, **kwargs):
            if not analyze_mode:
                return fn(*args, **kwargs)

            result = [{"topics": self.topics} | {"kind": type(self)}]
            try:
                result += fn(*args, analyze_mode=analyze_mode, **kwargs)
            except Exception:  # pragma: no cover
                pass
            return result

        return wrapper


class BrokerQueryEnroute:
    """Broker Query Enroute class"""

    def __init__(self, topics: list[str], **kwargs):
        self.kwargs = kwargs
        self.topics = topics

    def __call__(self, fn):
        def wrapper(*args, analyze_mode: bool = False, **kwargs):
            if not analyze_mode:
                return fn(*args, **kwargs)

            result = [{"topics": self.topics} | {"kind": type(self)}]
            try:
                result += fn(*args, analyze_mode=analyze_mode, **kwargs)
            except Exception:  # pragma: no cover
                pass
            return result

        return wrapper


class BrokerEventEnroute:
    """Broker Event Enroute class"""

    def __init__(self, topics: list[str], **kwargs):
        self.kwargs = kwargs
        self.topics = topics

    def __call__(self, fn):
        def wrapper(*args, analyze_mode: bool = False, **kwargs):
            if not analyze_mode:
                return fn(*args, **kwargs)

            result = [{"topics": self.topics} | {"kind": type(self)}]
            try:
                result += fn(*args, analyze_mode=analyze_mode, **kwargs)
            except Exception:  # pragma: no cover
                pass
            return result

        return wrapper


class BrokerEnroute:
    """Broker Enroute class"""

    command = BrokerCommandEnroute
    query = BrokerQueryEnroute
    event = BrokerEventEnroute


class RestCommandEnroute:
    """Rest Command Enroute class"""

    def __init__(self, topics: list[str], **kwargs):
        self.kwargs = kwargs
        self.topics = topics

    def __call__(self, fn):
        def wrapper(*args, analyze_mode: bool = False, **kwargs):
            if not analyze_mode:
                return fn(*args, **kwargs)

            result = [{"topics": self.topics} | {"kind": type(self)}]
            try:
                result += fn(*args, analyze_mode=analyze_mode, **kwargs)
            except Exception:  # pragma: no cover
                pass
            return result

        return wrapper


class RestQueryEnroute:
    """Rest Query Enroute class"""

    def __init__(self, url: str, method: str):
        self.url = url
        self.method = method

    def __call__(self, fn):
        def wrapper(*args, analyze_mode: bool = False, **kwargs):
            if not analyze_mode:
                return fn(*args, **kwargs)

            result = [self]
            try:
                result += fn(*args, analyze_mode=analyze_mode, **kwargs)
            except Exception:  # pragma: no cover
                pass
            return result

        return wrapper


class RestEnroute:
    """Rest Enroute class"""

    command = RestCommandEnroute
    query = RestQueryEnroute


class Enroute:
    """Enroute decorator main class"""

    broker = BrokerEnroute
    rest = RestEnroute


enroute = Enroute


def find_decorators(target):
    import ast
    import inspect

    res = {}

    def visit_FunctionDef(node):
        res[node.name] = bool(node.decorator_list)

    V = ast.NodeVisitor()
    V.visit_FunctionDef = visit_FunctionDef
    V.visit(compile(inspect.getsource(target), "?", "exec", ast.PyCF_ONLY_AST))
    return res

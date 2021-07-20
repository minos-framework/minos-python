# Copyright (C) 2020 Clariteia SL
#
# This file is part of minos framework.
#
# Minos framework can not be copied and/or distributed without the express
# permission of Clariteia SL.


class BaseDecorator:
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


class BrokerCommandEnroute(BaseDecorator):
    """Broker Command Enroute class"""

    def __init__(self, topics: list[str]):
        self.topics = topics


class BrokerQueryEnroute(BaseDecorator):
    """Broker Query Enroute class"""

    def __init__(self, topics: list[str]):
        self.topics = topics


class BrokerEventEnroute(BaseDecorator):
    """Broker Event Enroute class"""

    def __init__(self, topics: list[str]):
        self.topics = topics


class BrokerEnroute:
    """Broker Enroute class"""

    command = BrokerCommandEnroute
    query = BrokerQueryEnroute
    event = BrokerEventEnroute


class RestCommandEnroute(BaseDecorator):
    """Rest Command Enroute class"""

    def __init__(self, topics: list[str]):
        self.topics = topics


class RestQueryEnroute(BaseDecorator):
    """Rest Query Enroute class"""

    def __init__(self, url: str, method: str):
        self.url = url
        self.method = method


class RestEnroute:
    """Rest Enroute class"""

    command = RestCommandEnroute
    query = RestQueryEnroute


class Enroute:
    """Enroute decorator main class"""

    broker = BrokerEnroute
    rest = RestEnroute


enroute = Enroute


class EnrouteDecoratorAnalyzer:
    """Search decorators in specified class"""

    @classmethod
    def find_in_class(cls, classname) -> dict:
        fns = cls._find_decorators(classname)
        result = {}
        for name, decorated in fns.items():
            if not decorated:
                continue
            result[name] = getattr(classname, name)(analyze_mode=True)

        return result

    @staticmethod
    def _find_decorators(target):
        """Search decorators in a given class
        Original source: https://stackoverflow.com/a/9580006/3921457
        """
        import ast
        import inspect

        res = {}

        def visit_function_def(node):
            res[node.name] = bool(node.decorator_list)

        v = ast.NodeVisitor()
        v.visit_FunctionDef = visit_function_def
        v.visit(compile(inspect.getsource(target), "?", "exec", ast.PyCF_ONLY_AST))
        return res

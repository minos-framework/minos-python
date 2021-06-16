"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""


def property_or_classproperty(func):
    """Decorator to use class properties"""
    if not isinstance(func, (classmethod, staticmethod)):
        func = self_or_classmethod(func)

    return _ClassPropertyDescriptor(func)


def classproperty(func):
    """Decorator to use class properties"""
    if not isinstance(func, (classmethod, staticmethod)):
        func = classmethod(func)

    return _ClassPropertyDescriptor(func)


class _ClassPropertyDescriptor:
    def __init__(self, fget, fset=None):
        self.fget = fget
        self.fset = fset

    def __get__(self, obj, klass=None):
        return self.fget.__get__(obj, klass)()


# noinspection PyPep8Naming
class self_or_classmethod(classmethod):
    """Decorator to use the same method as instance or as class based on the caller."""

    # noinspection PyMethodOverriding
    def __get__(self, instance, type_):
        # noinspection PyUnresolvedReferences
        get = super().__get__ if instance is None else self.__func__.__get__
        # noinspection PyArgumentList
        return get(instance, type_)

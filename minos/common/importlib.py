"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

import importlib
import typing as t

import six

from .exceptions import (
    MinosImportException,
)


def import_module(module: str) -> t.Type:
    """Import the given module from a package"""
    try:
        if "." in module:
            parts = module.split(".")
            name = ".".join(parts[:-1])

        module_ref = importlib.import_module(name)
        kallable = getattr(module_ref, parts[-1])
        if not six.callable(kallable):
            raise TypeError("The module is not callable")
        return kallable
    except ImportError as e:
        raise MinosImportException("Error importing Package")


def classname(cls: t.Type) -> str:
    """Compute the given class full name.

    :param cls: Target class.
    :return: An string object.
    """
    if cls.__module__ in (None, "__builtin__"):
        return cls.__qualname__
    return f"{cls.__module__}.{cls.__qualname__}"

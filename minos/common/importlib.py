"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

import importlib
from typing import (
    Callable,
)

from .exceptions import (
    MinosImportException,
)


def import_module(module_name: str) -> Callable:
    """Import the given module from a package"""
    module_name, qualified_name = module_name.rsplit(".", 1)

    try:
        module = importlib.import_module(module_name)
    except ImportError:
        raise MinosImportException("Error importing Package: the module does not exist")

    try:
        # noinspection SpellCheckingInspection
        kallable = getattr(module, qualified_name)
    except AttributeError:
        raise MinosImportException("Error importing Package: the qualname does not exist.")

    return kallable


def classname(cls: Callable) -> str:
    """Compute the given class full name.

    :param cls: Target class.
    :return: An string object.
    """
    # noinspection PyUnresolvedReferences
    return f"{cls.__module__}.{cls.__qualname__}"

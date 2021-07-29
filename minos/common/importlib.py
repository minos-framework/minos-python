"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

import importlib
from types import (
    ModuleType,
)
from typing import (
    Callable,
)

from .exceptions import (
    MinosImportException,
)


# noinspection SpellCheckingInspection
def import_module(module_name: str) -> Callable:
    """Import the given module from a package"""
    parts = module_name.rsplit(".", 1)

    try:
        kallable = importlib.import_module(parts[0])
    except ImportError:
        raise MinosImportException(f"Error importing {module_name!r}: the module does not exist")

    if len(parts) > 1:
        try:
            kallable = getattr(kallable, parts[1])
        except AttributeError:
            raise MinosImportException(f"Error importing {module_name!r}: the qualname does not exist.")

    return kallable


def classname(cls: Callable) -> str:
    """Compute the given class full name.

    :param cls: Target class.
    :return: An string object.
    """
    if isinstance(cls, ModuleType):
        return cls.__name__
    # noinspection PyUnresolvedReferences
    return f"{cls.__module__}.{cls.__qualname__}"

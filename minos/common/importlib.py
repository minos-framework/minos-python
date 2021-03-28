import importlib
import typing as t

import six

from minos.common.exceptions import MinosImportException


def import_module(module: str) -> t.Callable:
    """Import the given module from a package"""
    try:
        if '.' in module:
            parts = module.split('.')
            name = '.'.join(parts[:-1])

        module_ref = importlib.import_module(name)
        kallable = getattr(module_ref, parts[-1])
        if not six.callable(kallable):
            raise TypeError("The module is not callable")
        return kallable
    except ImportError as e:
        raise MinosImportException("Error importing Package")

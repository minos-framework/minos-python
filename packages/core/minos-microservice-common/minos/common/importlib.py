import importlib
from types import (
    ModuleType,
)
from typing import (
    Callable,
    Union,
)

from .exceptions import (
    MinosImportException,
)


# noinspection SpellCheckingInspection
def import_module(module_name: str) -> Union[type, Callable, ModuleType]:
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


def classname(cls: Union[type, Callable]) -> str:
    """Compute the given class full name.

    :param cls: Target class.
    :return: An string object.
    """
    if isinstance(cls, ModuleType):
        return cls.__name__
    # noinspection PyUnresolvedReferences
    return f"{cls.__module__}.{cls.__qualname__}"


def get_internal_modules() -> list[ModuleType]:
    """Get the list of internal ``minos`` modules.

    :return: A list of modules.
    """
    import pkgutil

    modules = list()

    import minos
    for loader, module_name, _ in pkgutil.iter_modules(minos.__path__):
        module = importlib.import_module(f"minos.{module_name}")
        modules.append(module)

    import minos.plugins as minos_plugins
    for loader, module_name, _ in pkgutil.iter_modules(minos.plugins.__path__):
        module = importlib.import_module(f"minos.plugins.{module_name}")
        modules.append(module)
    return modules

import warnings
from abc import (
    ABC,
)
from collections import (
    defaultdict,
)
from pathlib import (
    Path,
)
from typing import (
    Union,
)

import yaml

from ..exceptions import (
    MinosConfigException,
)
from ..injections import (
    Injectable,
)


@Injectable("config")
class Config(ABC):
    """Config base class."""

    def __new__(cls, *args, **kwargs):
        if cls is not Config:
            return super().__new__(cls)

        from .v1 import (
            ConfigV1,
        )

        version_mapper = defaultdict(
            lambda: ConfigV1,
            {
                1: ConfigV1,
            },
        )

        version = _get_version(*args, **kwargs)

        return super().__new__(version_mapper[version])


# noinspection PyUnusedLocal
def _get_version(path: Union[str, Path], *args, **kwargs) -> int:
    if isinstance(path, str):
        path = Path(path)
    if not path.exists():
        raise MinosConfigException(f"Check if this path: {path} is correct")

    with path.open() as file:
        data = yaml.load(file, Loader=yaml.FullLoader)

    return data.get("version", 1)


class MinosConfig(Config):
    """MinosConfig class."""

    def __new__(cls, *args, **kwargs):
        warnings.warn(f"{MinosConfig!r} has been deprecated. Use {Config} instead.", DeprecationWarning)
        return super().__new__(cls, *args, **kwargs)

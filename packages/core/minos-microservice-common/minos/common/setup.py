from __future__ import (
    annotations,
)

import logging
import warnings
from pathlib import (
    Path,
)
from typing import (
    Optional,
    Type,
    TypeVar,
    Union,
)

from .config import (
    Config,
)
from .exceptions import (
    NotProvidedException,
)
from .injections import (
    Inject,
)
from .object import (
    Object,
)

logger = logging.getLogger(__name__)

S = TypeVar("S")


class SetupMixin(Object):
    """Setup Mixin class."""

    def __init__(self, *args, already_setup: bool = False, **kwargs):
        super().__init__(**kwargs)
        self._already_setup = already_setup

    @property
    def already_setup(self) -> bool:
        """Already Setup getter.

        :return: A boolean value.
        """
        return self._already_setup

    @property
    def already_destroyed(self) -> bool:
        """Already Destroy getter.

        :return: A boolean value.
        """
        return not self._already_setup

    @classmethod
    @Inject()
    def from_config(cls: Type[S], config: Union[Config, Path], **kwargs) -> S:
        """Build a new instance from config.

        :param config: Config instance. If `None` is provided, default config is chosen.
        :param kwargs: Additional named arguments.
        :return: A instance of the called class.
        """
        if isinstance(config, Path):
            config = Config(config)

        logger.info(f"Building a {cls.__name__!r} instance from config...")
        return cls._from_config(config=config, **kwargs)

    @classmethod
    def _from_config(cls: Type[S], config: Config, **kwargs) -> S:
        return cls(**kwargs)

    async def __aenter__(self: S) -> S:
        await self.setup()
        return self

    async def setup(self) -> None:
        """Setup miscellaneous repository things.

        :return: This method does not return anything.
        """
        if not self._already_setup:
            logger.info(f"Setting up a {type(self).__name__!r} instance...")
            await self._setup()
            self._already_setup = True

    async def _setup(self) -> None:
        return

    async def __aexit__(self, exc_type, exc_value, exc_traceback):
        await self.destroy()

    async def destroy(self) -> None:
        """Destroy miscellaneous repository things.

        :return: This method does not return anything.
        """
        if self._already_setup:
            logger.info(f"Destroying a {type(self).__name__!r} instance...")
            await self._destroy()
            self._already_setup = False

    async def _destroy(self) -> None:
        """Destroy miscellaneous repository things."""

    def __del__(self):
        if not getattr(self, "already_destroyed", True):
            warnings.warn(
                f"A not destroyed {type(self).__name__!r} instance is trying to be deleted...", ResourceWarning
            )


class MinosSetup(SetupMixin):
    """Minos Setup class."""

    def __init__(self, *args, **kwargs):
        warnings.warn(f"{MinosSetup!r} has been deprecated. Use {SetupMixin} instead.", DeprecationWarning)
        super().__init__(*args, **kwargs)

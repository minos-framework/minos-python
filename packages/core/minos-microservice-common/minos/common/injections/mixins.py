from ..object import (
    Object,
)


class InjectableMixin(Object):
    """TODO"""

    _injectable_name: str

    @classmethod
    def _set_injectable_name(cls, name: str) -> None:
        """TODO"""
        cls._injectable_name = name

    @classmethod
    def get_injectable_name(cls) -> str:
        """TODO"""
        return cls._injectable_name

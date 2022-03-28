from ..setup import (
    SetupMixin,
)


class InjectableMixin(SetupMixin):
    """Injectable Mixin class."""

    _injectable_name: str

    @classmethod
    def _set_injectable_name(cls, name: str) -> None:
        cls._injectable_name = name

    @classmethod
    def get_injectable_name(cls) -> str:
        """Get the injectable name.

        :return: A ``str`` value.
        """
        return cls._injectable_name

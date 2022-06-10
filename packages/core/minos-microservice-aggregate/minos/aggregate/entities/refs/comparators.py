from functools import (
    lru_cache,
)
from typing import (
    get_origin,
)

from minos.common import (
    is_type_subclass,
)

from .models import (
    Ref,
)


@lru_cache()
def is_ref_subclass(type_: type) -> bool:
    """Check if the given type field is subclass of ``Ref``."""
    if not is_type_subclass(type_):
        type_ = get_origin(type_)
    return is_type_subclass(type_) and issubclass(type_, Ref)

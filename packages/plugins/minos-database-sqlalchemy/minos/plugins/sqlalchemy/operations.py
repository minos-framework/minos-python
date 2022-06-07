from collections.abc import (
    Callable,
)
from typing import (
    Any,
    Optional,
    Union,
)

from sqlalchemy import (
    text,
)
from sqlalchemy.engine import (
    Connectable,
)
from sqlalchemy.sql import (
    Executable,
)

from minos.common import (
    DatabaseOperation,
)


class SqlAlchemyDatabaseOperation(DatabaseOperation):
    """SqlAlchemy Database Operation class."""

    def __init__(
        self,
        statement: Union[Callable[[Connectable, ...], ...], str, Executable],
        parameters: Optional[dict[str, Any]] = None,
        stream: Optional[bool] = None,
        *args,
        **kwargs
    ):
        if stream is None:
            stream = True
        super().__init__(*args, **kwargs)

        if isinstance(statement, str):
            statement = text(statement)
        if parameters is None:
            parameters = dict()

        self.statement = statement
        self.parameters = parameters
        self.stream = stream

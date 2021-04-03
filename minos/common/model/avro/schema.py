import dataclasses
import typing as t


@dataclasses.dataclass
class SchemaMetadata:
    schema_doc: bool = True
    namespace: t.Optional[t.List[str]] = None
    aliases: t.Optional[t.List[str]] = None

    @classmethod
    def create(cls, klass: t.Any) -> t.Any:
        return cls(
            schema_doc=getattr(klass, "schema_doc", True),
            namespace=getattr(klass, "namespace", None),
            aliases=getattr(klass, "aliases", None),
        )

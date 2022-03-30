class PathPart:
    def __init__(self, name: str):
        self.name = name
        self.is_generic: bool = True if self.name.startswith("{") and self.name.endswith("}") else False


class Endpoint:
    def __init__(self, path: str):
        self.path: tuple[PathPart] = tuple(PathPart(path_part) for path_part in path.split("/"))

    @property
    def path_as_str(self) -> str:
        return "/".join([str(part.name) for part in self.path])

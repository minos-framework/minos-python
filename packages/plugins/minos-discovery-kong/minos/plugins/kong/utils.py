import re


class PathPart:
    def __init__(self, name: str):
        self.name = name
        self.is_generic: bool = True if self.name.startswith("{") and self.name.endswith("}") else False


class Endpoint:
    part_path_pattern = r"\{\S+:.+\}"

    def __init__(self, path: str):
        self.path: tuple[PathPart] = tuple(PathPart(path_part) for path_part in path.split("/"))

    @property
    def path_as_str(self) -> str:
        list_parts = []
        for part in self.path:
            if part.is_generic:
                part.name = ".*"
            list_parts.append(str(part.name))
        return "/".join(list_parts)

    @property
    def path_as_regex(self) -> str:
        list_parts = []
        for part in self.path:
            if part.is_generic:
                if re.match(self.part_path_pattern, part.name):
                    regex = part.name.split(":")[1]
                    regex = regex[:-1]
                    list_parts.append(regex)
                else:
                    list_parts.append(".*")
            else:
                list_parts.append(part.name)
        return "/".join(list_parts)

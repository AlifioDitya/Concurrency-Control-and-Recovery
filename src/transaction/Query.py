from typing import Callable, List

from .FileHandler import FileHandler
from .BinaryData import BinaryData

class Query:
    def __init__(self, *file_names: str) -> None:
        self.file_handlers: List[FileHandler] = [FileHandler(filename) for filename in file_names]

    def execute(self, *args: BinaryData) -> None:
        raise NotImplementedError()


class ReadQuery(Query):
    def execute(self, *args: BinaryData) -> None:
        for data, file_handler in zip(args, self.file_handlers):
            data.set_value(file_handler.read())


class WriteQuery(Query):
    def execute(self, *args: BinaryData) -> None:
        for data, file_handler in zip(args, self.file_handlers):
            file_handler.write(data.get_value())


class FunctionQuery(Query):
    def __init__(self, *file_names: str, function: Callable[..., int] = lambda *args: args[0]) -> None:
        super().__init__(*file_names)
        self.function = function

    def execute(self, *args: BinaryData) -> None:
        values = [data.get_value() for data in args]
        args[0].set_value(self.function(*values))


class DisplayQuery(Query):
    def __init__(self, *file_names: str, function: Callable[..., int] = lambda *args: args) -> None:
        super().__init__(*file_names)
        self.function = function

    def execute(self, *args: BinaryData) -> None:
        values = [data.get_value() for data in args]
        result = self.function(*values)

        if isinstance(result, tuple):
            print(" ".join(map(str, result)))
        elif isinstance(result, int):
            print(result)

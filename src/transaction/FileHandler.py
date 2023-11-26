import math

class FileHandler:
    def __init__(self, filename: str) -> None:
        self.filename: str = filename

    def read(self) -> int:
        """
        Read an integer value from the file.
        """
        with open(self.filename, "rb") as file:
            content = file.read()

        return int.from_bytes(content, byteorder="big", signed=False)

    def write(self, value: int) -> None:
        """
        Write an integer value to the file.
        """
        with open(self.filename, "wb") as file:
            length = math.ceil(value / (1 << 8))
            content = value.to_bytes(length, byteorder="big", signed=False)
            file.write(content)

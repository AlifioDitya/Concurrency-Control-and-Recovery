class BinaryData:
    '''
    BinaryData is a class that represents a binary data.

    Attributes:
        value (int): The value of the data.
    '''
    def __init__(self) -> None:
        self.value: int = 0

    def get_value(self) -> int:
        return self.value

    def set_value(self, value: int) -> None:
        self.value = value
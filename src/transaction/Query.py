from typing import List

class Query:
    def __init__(self, item: str) -> None:
        self.item: str = item
    
    def get_data_item(self) -> List[str]:
        return self.item
    
    def show(self) -> None:
        raise NotImplementedError()

class Read(Query):
    def show(self, timestamp: int) -> None:
        print(f"R{timestamp}({self.item})")

class Write(Query):
    def show(self, timestamp: int) -> None:
        print(f"W{timestamp}({self.item})")
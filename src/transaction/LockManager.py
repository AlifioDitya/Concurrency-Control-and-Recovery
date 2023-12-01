import enum

class LockType(enum.Enum):
    X = 1                       # exclusive lock
    S = 2                       # shared lock

    def __lt__ (self, other : 'LockType'):
        return self.value < other.value
    
    def __gt__ (self, other : 'LockType'):
        return self.value > other.value
    
    def __eq__ (self, other : 'LockType'):
        return self.value == other.value

class LockManager:
    def __init__(self):
        self.locks_table = {}

    def lock_data(self, transaction_id: int, data_item: str, lock_type: LockType):
        self.locks_table[data_item] = lock_type
        self.locks_table[(transaction_id, data_item)] = lock_type

    def unlock(self, transaction_id: int):
        keys_to_remove = [t for t in self.locks_table if 
                          isinstance(t, tuple) and t[0] == transaction_id]
        for key in keys_to_remove:
            if type(key) == tuple and key[0] == transaction_id:
                self.unlock_data(transaction_id, key[1])
        return True

    def unlock_data(self, transaction_id: int, data_item: str):
        if (transaction_id, data_item) in self.locks_table:
            del self.locks_table[(transaction_id, data_item)]
            if data_item in self.locks_table:
                del self.locks_table[data_item]

    def is_locked(self, data_item: str) -> bool:
        return data_item in self.locks_table
            
    def is_locked_by(self, transaction_id: int) -> bool:
        return transaction_id in [t[0] for t in self.locks_table]
        
    def has_lock(self, transaction_id: int, data_item: str) -> bool:
        return (transaction_id, data_item) in self.locks_table
    
    def has_lock_type(self, transaction_id: int, data_item: str, lock_type: LockType) -> bool:
        return self.locks_table[(transaction_id, data_item)] == lock_type
    
    def lock_type(self, data_item: str) -> LockType:
        return self.locks_table[data_item]
    
    def upgrade_lock(self, transaction_id: int, data_item: str, lock_type: LockType):
        self.locks_table[data_item] = lock_type
        self.locks_table[(transaction_id, data_item)] = lock_type

    def is_lock_shared(self, data_item: str) -> bool:
        return len([t for t in self.locks_table if 
                   isinstance(t, tuple) and t[1] == data_item]) > 1
    
    def get_transaction_ids(self, data_item: str) -> list:
        return [t[0] for t in self.locks_table if 
                isinstance(t, tuple) and t[1] == data_item]
    
    def get_lock_table(self):
        # return lock table with key tuple only
        return {k: self.locks_table[k] for k in self.locks_table if isinstance(k, tuple)}
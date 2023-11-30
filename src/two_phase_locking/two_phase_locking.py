# two phase locking concurrency control
# Rigorouse two phase locking protocol
# unlock after commit

# format
# <Operation><Transaction ID>(<Data item>)) ;
# <Operation> = R | W | C
# <Transaction ID> = number
# <Data item> = letter and optional

# example
# R1(X); W2(X); W2(Y); W3(Y); W1(X); C1; C2; C3;

# output the final schedule after two phase locking

import enum

class Operation(enum.Enum):
    READ = 1                    # read
    WRITE = 2                   # write
    COMMIT = 3                  # commit

class LockType(enum.Enum):
    X = 1                       # exclusive lock
    S = 2                       # shared lock

    def __lt__ (self, other : 'LockType'):
        return self.value < other.value
    
    def __gt__ (self, other : 'LockType'):
        return self.value > other.value
    
    def __eq__ (self, other : 'LockType'):
        return self.value == other.value

def parse_input(input: str) -> list:
    input = input.split(";")
    input = [s.strip() for s in input]
    input = [s for s in input if s != '']
    return input

def parse_schedule(schedule: list) -> list:
    # parse the schedule
    # return a list of tuple (operation, transaction_id, data_item)
    result = []
    s : str
    for s in schedule:
        match s[0].upper():
            case "R":
                operation = Operation.READ
            case "W":
                operation = Operation.WRITE
            case "C":
                operation = Operation.COMMIT
            case _:
                raise Exception("Invalid operation")
        transaction_id = int(s[1])
        data_item = s[3:-1]
        result.append((operation, transaction_id, data_item))
    return result

class LockManager:
    def __init__(self):
        self.locks_table = {}

    def lock_data(self, transaction_id: int, data_item: str, lock_type: LockType):
        self.locks_table[data_item] = lock_type
        self.locks_table[(transaction_id, data_item)] = lock_type

    def unlock(self, transaction_id: int):
        keys_to_remove = [trans for trans in self.locks_table if 
                          isinstance(trans, tuple) and trans[0] == transaction_id]
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


class TwoPhaseLocking:
    def __init__(self, schedule: list, lockmanager: LockManager = LockManager()):
        self.schedule = schedule
        self.parsed_schedule = parse_schedule(schedule)
        self.locks_manager = lockmanager
        self.waiting_queue = []
        self.result = []
    
    def add_queue(self, operation: Operation, transaction_id: int, data_item: str):
        self.waiting_queue.append((operation, transaction_id, data_item))

    def is_waiting(self, transaction_id: int) -> bool:
        return transaction_id in [t[1] for t in self.waiting_queue]

    def remove_queue(self, transaction_id: int):
        self.waiting_queue = [t for t in self.waiting_queue if t[1] != transaction_id]

    def add_result(self, operation: Operation, transaction_id: int, data_item: str=""):
        transaction_id = str(transaction_id)
        op_str = "R" if operation == Operation.READ else "W" if operation == Operation.WRITE else "C"
        result_string = op_str + transaction_id
        if data_item != "":
            result_string += "(" + data_item + ")"
        self.result.append(result_string)

    def add_lock_result(self, transaction_id: int, data_item: str, lock_type: LockType):
        transaction_id = str(transaction_id)
        operation: str = "SL" if lock_type == LockType.S else "XL"
        self.result.append(operation + transaction_id + "(" + data_item + ")")

    def add_upgrade_result(self, transaction_id: int, data_item: str, lock_type: LockType):
        transaction_id = str(transaction_id)
        operation = "XU" if lock_type == LockType.X else "SU"
        self.result.append(operation + transaction_id + "(" + data_item + ")")

    def add_unlock_result(self ,transaction_id: int, data_item: str):
        transaction_id = str(transaction_id)
        self.result.append("UL" + transaction_id + "(" + data_item + ")")

    def queue_to_schedule(self):
        to_add = []

        while self.waiting_queue:
            operation, transaction_id, data_item = self.waiting_queue.pop(0)
            to_add.append((operation, transaction_id, data_item))

        self.parsed_schedule = to_add + self.parsed_schedule 

        self.waiting_queue = []    

    
    def print_operation(self, operation: Operation, transaction_id: int, data_item: str):
        if data_item == "":
            print(operation , " Transaction ID: ", transaction_id)
        else:
            print(operation , " Transaction ID: ", transaction_id, " Data Item: ", data_item)
        

    def run(self, upgrade=True):
        while self.parsed_schedule:

            operation : Operation
            transaction_id : int
            data_item : str
            operation, transaction_id, data_item = self.parsed_schedule.pop(0) 
            # self.print_current_operation(operation, transaction_id, data_item)

            match operation:
                case Operation.READ | Operation.WRITE:
                    lock_type = LockType.X if operation == Operation.WRITE or not upgrade else LockType.S

                    if self.is_waiting(transaction_id):
                        self.add_queue(operation, transaction_id, data_item)

                    elif self.locks_manager.is_locked(data_item):
                        if self.locks_manager.has_lock(transaction_id, data_item):
                            if self.locks_manager.has_lock_type(transaction_id, data_item, lock_type):
                                self.add_result(operation, transaction_id, data_item)

                            elif upgrade:
                                if self.locks_manager.lock_type(data_item) > lock_type:
                                    self.locks_manager.upgrade_lock(transaction_id, data_item, lock_type)
                                    self.add_upgrade_result(transaction_id, data_item, lock_type)
                                self.add_result(operation, transaction_id, data_item)

                        elif lock_type == self.locks_manager.lock_type(data_item) == LockType.S and upgrade:
                            self.locks_manager.lock_data(transaction_id, data_item, lock_type)
                            self.add_lock_result(transaction_id, data_item, lock_type)
                            self.add_result(operation, transaction_id, data_item)
    
                        else:
                            self.add_queue(operation, transaction_id, data_item)
                    else:
                        self.locks_manager.lock_data(transaction_id, data_item, lock_type)
                        self.add_lock_result(transaction_id, data_item, lock_type)
                        self.add_result(operation, transaction_id, data_item)

                case Operation.COMMIT:
                    if self.is_waiting(transaction_id):
                        self.add_queue(operation, transaction_id, data_item)
                    else:
                        self.add_result(operation, transaction_id, data_item)
                        if self.locks_manager.is_locked_by(transaction_id):
                            for data_item in self.locks_manager.locks_table:
                                if type(data_item) == tuple and data_item[0] == transaction_id:
                                    self.add_unlock_result(transaction_id, data_item[1])
                            self.locks_manager.unlock(transaction_id)
                        self.queue_to_schedule()

                case _:
                    raise Exception("Invalid operation")
        
        #todo detect deadlock
        # raise error if deadlock
        if len(self.waiting_queue) > 0:
            raise Exception("Deadlock detected")

if __name__ == "__main__":

    sample_input_1 = "R1(X); W2(X); W2(Y); W3(Y); W1(X); C1; C2; C3;"
    sample_input_2 = "R1(X); R2(Y); R1(Y); W2(Y); W1(X); C1; C2;"
    sample_input_3 = "R1(X); R2(Y); R1(Y); R2(X); C1; C2;"            # no deadlock because shared unless not upgrade
    sample_input_4 = "R1(X) ;W2(Y) ;W2(X); W3(Y) ;W1(Y); C1; C2; C3;" # deadlock

    schedule = parse_input(sample_input_3)

    try: 
        transaction = TwoPhaseLocking(schedule)
        transaction.run(upgrade=True)
        print(transaction.result)
    except Exception as e:
        print(e)



    
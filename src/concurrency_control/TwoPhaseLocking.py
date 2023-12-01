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

    def __str__(self):
        return self.name
    
    def __repr__(self):
        return self.__str__()

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
    
class TwoPhaseLocking:
    def __init__(self, schedule: list, lockmanager: LockManager = LockManager()):
        self.schedule = schedule
        self.parsed_schedule = parse_schedule(schedule)
        self.locks_manager = lockmanager
        self.waiting_queue = []
        self.result = []
        self.upgrade = False
        self.rollback = False
        self.verbose = False
    
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

    def add_rollback_result(self, transaction_id: int, data_item: str):
        transaction_id = str(transaction_id)
        self.result.append("RB" + transaction_id + "(" + data_item + ")")

    def queue_to_schedule(self):
        to_add = []

        while self.waiting_queue:
            operation, transaction_id, data_item = self.waiting_queue.pop(0)
            to_add.append((operation, transaction_id, data_item))

        self.parsed_schedule = to_add + self.parsed_schedule 

        self.waiting_queue = []    

    def print_operation(self, operation: Operation, transaction_id: int, data_item: str):
        out = "Current Operation: "
        out += str(operation) + " " + str(transaction_id)
        if data_item != "":
            out += " (" + data_item + ")"
        print(out)

    def print_result(self):
        print("Final Schedule:")
        for r in self.result:
            print(r, end="; ")

    def print_state(self):
        print("Schedule: ", self.parsed_schedule)
        print("Locks Table: ", self.locks_manager.get_lock_table())
        print("Waiting Queue: ", self.waiting_queue)
        print("Result: ", self.result)


    def rollback_transaction(self, transaction_id: int, data_item: str):
        # get schedule from result for transaction id until locking
        p_schedule = [t for t in self.schedule if str(transaction_id) in t]
        t_schedule = [t for t in self.result if str(transaction_id) in t]
                    
        index = -1
        for i in range(len(t_schedule)):
            if "XL" + str(transaction_id) + "(" + data_item + ")" in t_schedule[i] or "SL" + str(transaction_id) + "(" + data_item + ")" in t_schedule[i]:
                index = i
                break
    
        t_remove = t_schedule[index:]
    
        index = -1
        for i in range(len(p_schedule)):
            if p_schedule[i] in t_remove:
                index = i
                break

        p_add = p_schedule[index:]
        p_add = parse_schedule(p_add)

        # print(t_remove)
        # print(p_add)
        
        # print(self.result)
        # print(self.parsed_schedule)
        self.result = [t for t in self.result if t not in t_remove]
        self.parsed_schedule = [t for t in self.parsed_schedule if t not in p_add]
        # print(self.result)
        # print(self.parsed_schedule)
        
        self.add_rollback_result(transaction_id, data_item)
        # for data_item in [t[1] for t in self.locks_manager.locks_table if t[0] == transaction_id]:
        #     self.add_unlock_result(transaction_id, data_item)
        self.locks_manager.unlock(transaction_id)
        # for t in p_add:
        #     self.add_queue(t[0], t[1] , t[2])

    def wound_wait(self, transaction_id :int, data_item:str):
        conflicting_transactions = self.locks_manager.get_transaction_ids(data_item) + [transaction_id]
        oldest_transaction = min(conflicting_transactions)
        younger_transactions = [t for t in conflicting_transactions if t > oldest_transaction]

        print("Wound wait: ", transaction_id, data_item, oldest_transaction, younger_transactions)

        if oldest_transaction < transaction_id:
            print("Add to queue")
            self.add_queue(Operation.WRITE, transaction_id, data_item)
            return
        
        for t in younger_transactions:
            print("Rollback: ", t, data_item)
            self.rollback_transaction(t, data_item)

        self.parsed_schedule.insert(0, (Operation.WRITE, transaction_id, data_item))
            

    def process_read_write(self, 
        operation: Operation, transaction_id: int, 
        data_item: str,lock_type: LockType):

        if self.is_waiting(transaction_id):
            self.add_queue(operation, transaction_id, data_item)
            return

        if self.locks_manager.is_locked(data_item):
            if self.locks_manager.has_lock(transaction_id, data_item):
                if self.locks_manager.has_lock_type(transaction_id, data_item, lock_type):
                    self.add_result(operation, transaction_id, data_item)
                    return

                if self.upgrade and self.locks_manager.lock_type(data_item) > lock_type and not self.locks_manager.is_lock_shared(data_item):
                    self.locks_manager.upgrade_lock(transaction_id, data_item, lock_type)
                    self.add_upgrade_result(transaction_id, data_item, lock_type)
                    self.add_result(operation, transaction_id, data_item)
                    return

            if self.upgrade and lock_type == self.locks_manager.lock_type(data_item) == LockType.S:
                self.locks_manager.lock_data(transaction_id, data_item, lock_type)
                self.add_lock_result(transaction_id, data_item, lock_type)
                self.add_result(operation, transaction_id, data_item)
                return
            
            if self.rollback:
                self.wound_wait(transaction_id, data_item)
                return
            
            self.add_queue(operation, transaction_id, data_item)
            return

        self.locks_manager.lock_data(transaction_id, data_item, lock_type)
        self.add_lock_result(transaction_id, data_item, lock_type)
        self.add_result(operation, transaction_id, data_item)


    def process_commit(self, transaction_id: int, operation: Operation, data_item: str):
        if self.is_waiting(transaction_id):
            self.add_queue(operation, transaction_id, data_item)
            return

        self.add_result(operation, transaction_id, data_item)
        if self.locks_manager.is_locked_by(transaction_id):
            for data_item in [t[1] for t in self.locks_manager.locks_table if t[0] == transaction_id]:
                self.add_unlock_result(transaction_id, data_item)
            self.locks_manager.unlock(transaction_id)
            self.queue_to_schedule()


    def run(self, upgrade=False, rollback=False, verbose=False):
        self.upgrade = upgrade
        self.rollback = rollback
        self.verbose = verbose
        while len(self.parsed_schedule) > 0:
    
            operation : Operation
            transaction_id : int
            data_item : str
            operation, transaction_id, data_item = self.parsed_schedule.pop(0) 


            match operation:
                case Operation.READ | Operation.WRITE:
                    lock_type = LockType.X if operation == Operation.WRITE or not self.upgrade else LockType.S
                    self.process_read_write(operation, transaction_id, data_item, lock_type)

                case Operation.COMMIT:
                    self.process_commit(transaction_id, operation, data_item)

                case _:
                    raise Exception("Invalid operation")
                
            if self.verbose:
                self.print_operation(operation, transaction_id, data_item)
                self.print_state()
                print()
        
        # raise error if deadlock
        if len(self.waiting_queue) > 0:
            raise Exception("Deadlock detected")

if __name__ == "__main__":

    sample_input_1 = "R1(X); W2(X); W2(Y); W3(Y); W1(X); C1; C2; C3;"
    sample_input_2 = "R1(X); R2(Y); R1(Y); W2(Y); W1(X); C1; C2;"
    sample_input_3 = "R1(X); R2(Y); R1(Y); R2(X); C1; C2;"            # deadlock if simple
    sample_input_4 = "R1(X) ;W2(Y) ;W2(X); W3(Y) ;W1(Y); C1; C2; C3;" # deadlock
    sample_input_5 = "R1(X); R2(X); W2(Y); W3(Y); W1(X); C1; C2; C3"
    sample_input_6 = "W1(X); W2(Y); W1(Y); W2(X); C1; C2"             # deadlock

    schedule = parse_input(sample_input_1)

    transaction = TwoPhaseLocking(schedule)
    transaction.run(upgrade=False, rollback=False, verbose=True)
    transaction.print_result()

    
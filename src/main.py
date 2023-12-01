from transaction.Query import Read, Write
from concurrency_control.OCC import OCCTransaction, OCC
from concurrency_control.TwoPhaseLocking import TwoPhaseLocking, parse_input, LockManager
from util.Util import menu_tpl, input_tpl, option_tpl, parse_input

ITEM_A = 'A'
ITEM_B = 'B'

def occ():
    T1 = OCCTransaction(1, [
        Read(ITEM_B),
        Write(ITEM_B),
        Read(ITEM_A),
        Write(ITEM_A)
    ])

    T2 = OCCTransaction(2, [
        Read(ITEM_B),
        Write(ITEM_B),
        Read(ITEM_A),
        Write(ITEM_A)
    ])

    concurrencyManager = OCC(
        [T1, T2],
        [1, 1, 2, 2, 2, 2, 1, 1]
    )

    concurrencyManager.run()

def two_phase():
    print("Two-phase simple lock with some feature")
    print()
    sample_input_1 = "R1(X); W2(X); W2(Y); W3(Y); W1(X); C1; C2; C3;"
    sample_input_6 = "W1(X); W2(Y); W1(Y); W2(X); C1; C2"             # deadlock

    schedule = input_tpl(menu_tpl() , sample_input_1)
    p_schedule = parse_input(schedule)
    print()

    option = option_tpl()
    upgrade = option[0]
    rollback = option[1]
    verbose = option[2]
    print()

    tpl = TwoPhaseLocking(p_schedule, LockManager())
    tpl.run(upgrade=upgrade, rollback=rollback, verbose=verbose)
    print()

    tpl.print_result()


if __name__ == "__main__":
    print("Welcome to the Concurrency Control Simulator!")
    print()
    print("Choose an algorithm to run:")
    print("1. Optimistic Concurrency Control")
    print("2. Two Phase Locking")
    print()
    choice = input("Enter your choice: ")
    print()

    if choice == "1":
        occ()
    elif choice == "2":
        two_phase()
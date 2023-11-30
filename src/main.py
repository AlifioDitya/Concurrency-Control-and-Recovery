from transaction.Query import Read, Write
from concurrency_control.OCC import OCCTransaction, OCC
from concurrency_control.TwoPhaseLocking import TwoPhaseLocking, parse_input, LockManager

ITEM_A = 'A'
ITEM_B = 'B'

def main():
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
    sample_input_1 = "R1(X); W2(X); W2(Y); W3(Y); W1(X); C1; C2; C3;"
    sample_input_6 = "W1(X); W2(Y); W1(Y); W2(X); C1; C2"             # deadlock

    s1 = parse_input(sample_input_1)
    s6 = parse_input(sample_input_6)

    t1 = TwoPhaseLocking(s1,LockManager())
    t6 = TwoPhaseLocking(s6,LockManager())

    t1.run()    
    print(t1.result)
    t6.run(rollback=True)
    print(t6.result)


if __name__ == "__main__":
    main()
    two_phase()
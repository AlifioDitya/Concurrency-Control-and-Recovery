from transaction.Query import Read, Write
from concurrency_control.OCC import OCCTransaction, OCC

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

if __name__ == "__main__":
    main()
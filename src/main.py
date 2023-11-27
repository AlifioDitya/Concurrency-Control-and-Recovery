from transaction.Query import ReadQuery, WriteQuery, FunctionQuery, DisplayQuery
from concurrency_control.OCC import OCCTransaction, OCC
import os

ROOT_DIR = os.path.realpath(os.path.join(os.path.dirname(__file__), '..'))
A_PATH = os.path.join(ROOT_DIR, "bin", "A.txt")
B_PATH = os.path.join(ROOT_DIR, "bin", "B.txt")

def main():
    T25 = OCCTransaction(25, [
        ReadQuery(B_PATH),
        DisplayQuery(B_PATH, function=lambda B: B),
        WriteQuery(B_PATH),
        ReadQuery(A_PATH),
        DisplayQuery(A_PATH, function=lambda A: A),
        WriteQuery(A_PATH)
    ])

    T26 = OCCTransaction(26, [
        ReadQuery(B_PATH),
        DisplayQuery(B_PATH, function=lambda B: B),
        FunctionQuery(B_PATH, function=lambda B: B + 50),
        DisplayQuery(B_PATH, function=lambda B: B),
        WriteQuery(B_PATH),
        ReadQuery(A_PATH),
        DisplayQuery(A_PATH, function=lambda A: A),
        FunctionQuery(A_PATH, function=lambda A: A + 50),
        DisplayQuery(A_PATH, function=lambda A: A),
        WriteQuery(A_PATH)
    ])

    concurrencyManager = OCC(
        [T25, T26],
        [25, 25, 25, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 25, 25, 25]
    )

    concurrencyManager.run()

if __name__ == "__main__":
    main()
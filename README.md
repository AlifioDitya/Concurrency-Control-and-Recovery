# Concurrency Control and Recovery

## Introduction

This project is a simple implementation of concurrency control mechanisms in a database management system.The project is written in Python. The source code is divided into two main parts. The first part is the implementation of a simple database transactions system, the second part is the implementation of concurrency control with two mechanisms: Two Phase Locking and Optimistic Concurrency Control.

## Database Transactions System

The database transactions system is implemented in the file `transaction.py`. The system supports the following operations:

* `read(x)`: Reads the value of variable `x` in the current transaction.
* `write(x)`: Writes to variable `x` in the current transaction.
* `commit()`: Commits the current transaction.
* `rollback()`: Rollbacks the current transaction.

## Concurrency Control

The concurrency control is implemented in the file `ConcurrencyControl.py` and is further implemented into two classes: `TwoPhaseLocking` and `OptimisticConcurrencyControl`. For simplicity reasons, only OCC inherits from the abstract class `ConcurrencyControl`.

### Two Phase Locking

Two Phase Locking is a concurrency control mechanism that uses locks to ensure serializability. It involves two phases: the growing phase and the shrinking phase. In the growing phase, transactions acquire locks and in the shrinking phase, transactions release locks. The implementation of Two Phase Locking is in the file `TwoPhaseLocking.py`.

Example input for two phase locking can be seen on \test folder (tpl1.txt and tpl2.txt)

### Optimistic Concurrency Control

Optimistic Concurrency Control is a concurrency control mechanism that does not use locks to ensure serializability. Instead, it uses a validation phase to check if the transactions are serializable. The implementation of Optimistic Concurrency Control is in the file `OptimisticConcurrencyControl.py`.

## Running the Code

The code can be run by executing the file `main.py`. The file contains a simple example of how the code can be used. The example is the same as the one in the project description. The code can be run with the following command:

```bash
python3 src/main.py 
```

The progam will prompt the user to select which concurrency control mechanism to simulate.

## Authors

* [Enrique Alifio Ditya](https://github.com/AlifioDitya)
* [Yanuar Sano nur Rasyid](https://github.com/yansans)

## Made with

![Python](https://camo.githubusercontent.com/94be0a2e5be142925615e5821d97137a930d08fc154962ce43860f1957e6661e/68747470733a2f2f696d672e736869656c64732e696f2f62616467652f507974686f6e2d3337373641423f7374796c653d666f722d7468652d6261646765266c6f676f3d707974686f6e266c6f676f436f6c6f723d7768697465)
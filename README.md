# lightest

Banking benchmark - emulate a bank account ledger using lightweight
transactions.

A nice property of a ledger which encapsulates all possible accounts is
that the total sum of all balances stays intact. This is used to fusion
correctness and performance/stress test into a single benchmark.

The tool can be used to populate accounts and calculate the inital balance,
or to make transfers (Zipfin and Normal distribution) and check that the
total amount is unchanged.

It also features an oracle mode, in which it checks all debits and credits,
as well as final balances, against a built-in oracle.

The number of workers, connections, accounts, and a few other properties
are configurable. 

## Building

The benchmark is written in Go, using mod vendoring technique, 
and a small Makefile is provided to simplify building. Simply clone/download
the source code, type 'make' and use  'lightest --help' to get started.

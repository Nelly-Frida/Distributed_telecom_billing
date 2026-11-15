# Week 1 Failure-Driven Engineering Log

## What Changed?
The initial distributed billing prototype was redesigned so that nodes are independent operating-system processes.

## What Failed?
The initial implementation represented distributed nodes as Python objects inside one process.

## Why Did It Fail?
The logical node abstraction did not create independent processes or network communication.

## How Was It Fixed?
Python subprocesses and TCP socket communication were introduced.

## Alternative Considered
A thread-based ThreadPoolExecutor design was considered, but it does not provide independent operating-system processes.

## What Was Learned?
Distributed components must be explicitly modeled as independent computational processes and communication endpoints.

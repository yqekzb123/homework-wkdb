WKDB
=======

WKDB is a testbed for evaluating concurrency control protocols.

Build & Test
------------

To build the database.

    make deps
    make -j

Run
---

The WKDB can be run with 

    ./rundb -nid0
    ./runcl -nid1

Configuration
-------------

DBMS configurations can be changed in the config.h file. Please refer to README for the meaning of each configuration. Here we only list several most important ones. 

    NODE_CNT          : Number of server nodes in the database
    THREAD_CNT        : Number of worker threads running per server
    WORKLOAD          : Supported workloads include YCSB, TPCC and TEST
    ALGO              : Concurrency control algorithm. Two algorithms are supported 
                        (OCCTEMPLATE, CALVIN) (You need to implement MVCC, OCC, TO, etc.) 
    MAX_TXN_IN_FLIGHT : Maximum number of active transactions at each server at a given time
    DONE_TIMER        : Amount of time to run experiment

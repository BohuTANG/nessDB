```
nessDB v3.0 with Buffered-Tree.
(C) 2012-2020 nessDB Authors ________________
_____________________________  __ \__  __ )
__  __ \  _ \_  ___/_  ___/_  / / /_  __  |
_  / / /  __/(__  )_(__  )_  /_/ /_  /_/ /
/_/ /_/\___//____/ /____/ /_____/ /_____/
================================================================
```

  nessDB is a fast Key-Value database(embedded), which is written in ANSI C with BSD LICENSE and works in most POSIX systems without external dependencies.

  ## FEATURES
  * Buffered-Tree index data structure
  * Range-Query
  * Transaction
  
  ## How To Bench
  Random write 10,000,000(10 Million) records with 512MB memory.
  
  ```
make db-bench
./db-bench --benchmarks=fillrandom --num=10000000 --cache_size=536870912
nessDB:     version 3.0.0
Date:       Sat Jun 20 19:06:42 2020
CPU:        4 *  Intel(R) Core(TM) i5-7200U CPU @ 2.50GHz
CPUCache:   3072 KB
Keys:       20 bytes each
Values:     100 bytes each
Entries:    10000000
RawSize:    1144.4 MB (estimated)
Compression:Snappy
WARNING: assertions are enabled; benchmarks unnecessarily slow
------------------------------------------------------------
random write finished 10000000 ops                              
[  t min,   t max]	  ops count	      %
--------------------------------------------------
[      0,       1]	    9998639	  99.99 
[      1,       2]	         92	   0.00 
[      2,       3]	         78	   0.00 
[      3,       4]	        100	   0.00 
[      4,       5]	         73	   0.00 
[      5,       6]	         60	   0.00 
[      6,       7]	         65	   0.00 
[      7,       8]	         79	   0.00 
[      8,       9]	         58	   0.00 
[      9,      10]	         58	   0.00 
[     10,      12]	         70	   0.00 
[     12,      14]	         72	   0.00 
[     14,      16]	         47	   0.00 
[     16,      18]	         35	   0.00 
[     18,      20]	         41	   0.00 
[     20,      25]	         40	   0.00 
[     25,      30]	         29	   0.00 
[     30,      35]	         24	   0.00 
[     35,      40]	         19	   0.00 
[     40,      45]	         31	   0.00 
[     45,      50]	         15	   0.00 
[     50,      60]	         42	   0.00 
[     60,      70]	         43	   0.00 
[     70,      80]	         47	   0.00 
[     80,      90]	         35	   0.00 
[     90,     100]	         39	   0.00 
[    100,     120]	         48	   0.00 
[    120,     140]	         19	   0.00 
[    140,     160]	          1	   0.00 
[    160,     180]	          1	   0.00 
--------------------------------------------------
total:       53514	   10000000	   100%
min latency       : 0.000000 * 1e-3 sec/op
avg latency       : 0.005351 * 1e-3 sec/op
max latency       : 171.927214 * 1e-3 sec/op
avg throughput    :  186866 ops/sec
```
  

  V3.0 is still cooking.
  For more work-in-process, please see TODO.


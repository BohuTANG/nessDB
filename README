   nessDB using Compact-Hash-Tree for Index-Structure in memory stream.Its features are Less-Memory and High-Performance.

   How to do
   =========
	a, %cd src
	b, %make
	c, %./nessdb_bench
	d, show the benchmark result
  
  
   Features
   ========
	a, Keys&values are arbitrary byte arrays.
	b, Basic operations are Put(key,value),Get(key),Remove(key).
	c, Keys are compacted to integer   in memory.

   Benchmark(100,0000 entries)
   ===========================
	Keys:		16 bytes each
	Values:		84 bytes each
	Entries:	1000000
	RawSize:	95.4 MB (estimated)
	FileSize:	103.0 MB (estimated)
	----------------------------------------------------------------------------------------------------------
	nessDB:		version 1.3
	Date:		Sat Jul 30 17:01:18 2011
	CPU:		2 *  Intel(R) Pentium(R) Dual  CPU  T3200  @ 2.00GHz
	CPUCache:	1024 KB

	+-----------------------+-------------------+----------------------------------------+-------------------+
	|write:			1.100000 micros/op; |	909090.909091 writes/sec(estimated); |	93.633478 MB/sec |
	+-----------------------+-------------------+----------------------------------------+-------------------+
	|readseq:		1.280000 micros/op; |	781250.000000 reads /sec(estimated); |	65.565109 MB/sec |
	+-----------------------+-------------------+----------------------------------------+-------------------+
	|readrandom:		4.920000 micros/op; |	203252.032520 reads /sec(estimated); |	17.057589 MB/sec |
	+-----------------------+-------------------+----------------------------------------+-------------------+

   Benchmark(1000,0000 entries)
   ===========================
	Keys:		16 bytes each
	Values:		84 bytes each
	Entries:	10000000
	RawSize:	953.7 MB (estimated)
	FileSize:	1030.0 MB (estimated)
	----------------------------------------------------------------------------------------------------------
	nessDB:		version 1.3
	Date:		Sat Jul 30 17:12:05 2011
	CPU:		2 *  Intel(R) Pentium(R) Dual  CPU  T3200  @ 2.00GHz
	CPUCache:	1024 KB

	+-----------------------+-------------------+----------------------------------------+-------------------+
	|write:			1.265000 micros/op; |	790513.833992 writes/sec(estimated); |	81.420416 MB/sec |
	+-----------------------+-------------------+----------------------------------------+-------------------+
	|readseq:		1.327000 micros/op; |	753579.502638 reads /sec(estimated); |	63.242909 MB/sec |
	+-----------------------+-------------------+----------------------------------------+-------------------+
	|readrandom:		5.123000 micros/op; |	195198.126098 reads /sec(estimated); |	16.381679 MB/sec |
	+-----------------------+-------------------+----------------------------------------+-------------------+

   

   Compared with LevelDB
   =====================
	LevelDB:    version 1.2
	Date:       Sat Jul 30 21:58:30 2011
	CPU:        2 * Intel(R) Pentium(R) Dual  CPU  T3200  @ 2.00GHz
	CPUCache:   1024 KB
	Keys:       16 bytes each
	Values:     84 bytes each (42 bytes after compression)
	Entries:    1000000
	RawSize:    95.4 MB (estimated)
	FileSize:   55.3 MB (estimated)
	WARNING: Snappy compression is not enabled
	------------------------------------------------
	fillseq      :       4.323 micros/op;   22.1 MB/s     
	fillsync     :     138.830 micros/op;    0.7 MB/s (1000 ops)
	fillrandom   :      11.006 micros/op;    8.7 MB/s     
	overwrite    :      13.645 micros/op;    7.0 MB/s     
	readrandom   :      24.577 micros/op;                 
	readrandom   :      17.483 micros/op;                 
	readseq      :       0.662 micros/op;  144.1 MB/s    
	readreverse  :       1.237 micros/op;   77.1 MB/s    
	compact      : 2799380.064 micros/op;
	readrandom   :      15.144 micros/op;                 
	readseq      :       0.646 micros/op;  147.7 MB/s    
	readreverse  :       1.241 micros/op;   76.9 MB/s    
	fill100K     :    6169.140 micros/op;   15.5 MB/s (1000 ops)
	crc32c       :       7.033 micros/op;  555.4 MB/s (4K per op)
	snappycomp   :       5.007 micros/op; (snappy failure)
	snappyuncomp :       4.053 micros/op; (snappy failure)
	acquireload  :       0.716 micros/op; (each op is 1000 loads)

       +----------------------------------------------------+
       / nessDB is 4X faster than LevelDB in "fillseq";     /
       / nessDB is 4X faster than LevelDB in "readrandom";  /
       / LevelDB is 2X faster than nessDB in "readseq";     /
       +----------------------------------------------------+

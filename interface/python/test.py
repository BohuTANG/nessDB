#!/usr/bin/env python
#-*- coding:utf-8 -*-
# author : KDr2
# BohuTANG @2012
#

import sys
import random
import string
import time
import nessdb

def gen_random_str(len):
	return ''.join([random.choice('abcdefghijklmnoprstuvwyxzABCDEFGHIJKLMNOPRSTUVWXYZ') for i in range(len)])

def ness_open(db_name):
	return nessdb.NessDB(db_name)

def ness_write(db, c):
	s_time = time.time()
	for i in range(0, c):
		key = gen_random_str(16)
		db.db_add(key, "abcd")
		if (i % 10000) == 0:
			sys.stdout.write("\r\x1b[K ....write finished " + i.__str__())
	 		sys.stdout.flush()

	e_time = time.time()
	print ""
	print "---->count:<%i>,cost time:<%i>, %i/sec\n"  %(c, e_time - s_time, c / (e_time - s_time))


if __name__ == '__main__':
	if (len(sys.argv) > 2):
		if (sys.argv[1] == "write"):
			db = ness_open("test")
			ness_write(db, int(sys.argv[2]))
			db.db_close()
	else:
		print "test.py write <count>"


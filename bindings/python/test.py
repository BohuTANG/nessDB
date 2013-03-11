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
	f = open('log/rw.txt', 'a+b')
	s_time = time.time()
	for i in range(0, c):
		key = gen_random_str(16)
		f.write(key+'\n')
		db.db_add(key, "abcd")
        	if (db.db_get(key) is None):
            		print "---------------------->not found........."

		if (i % 10000) == 0:
			sys.stdout.write("\r\x1b[K ....write finished " + i.__str__())
	 		sys.stdout.flush()
	f.close()
	e_time = time.time()
	print ""
	print "---->count:<%i>,cost time:<%i>, %i/sec\n"  %(c, e_time - s_time, c / (e_time - s_time))

def ness_check(db, c):
	i = 0
	found = 0
	f = open('log/rw.txt', 'r+')
	fno = open('log/error.txt', 'w')
	while 1:
    		line = f.readline()
    		if not line:
        		break

    		if (db.db_exists(line[:-1]) == 1):
        		found+=1
    		else:
        		print "oops...............%s", line
        		fno.write(line)

    		if (i% 10000) == 0:
        		sys.stdout.write("\r\x1b[K ....check finished " + i.__str__())
        		sys.stdout.flush()
    		i +=1

	print "found:", found
	fno.write('%d:%d' % (i,found))
	fno.close()

if __name__ == '__main__':
	if (len(sys.argv) > 2):
		if (sys.argv[1] == "write"):
			db = ness_open("ndbs")
			ness_write(db, int(sys.argv[2]))
			db.db_close()
		if (sys.argv[1] == "check"):
			db = ness_open("ndbs")
			ness_check(db, int(sys.argv[2]))
			db.db_close()
		if (sys.argv[1] == "shrink"):
			db = ness_open("ndbs")
			print 'to do shrink...'
			db.db_shrink()
			db.db_close()
	else:
		print "test.py write <count>"

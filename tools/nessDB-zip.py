#!/usr/bin/env python
#
# The BSD License
# 
# Copyright (c) 2012 BohuTANG <overred.shuttler at gmail dot com>
# 

"""
	nessDB's index file(*.sst)  is no fragmentation
	but the data file(ness.db) maybe fragment, because it's AOF. 
	So this tiny tool is to deal with this.
	I guess you should never use this tool, only when your 'ness.db' file reaches TBs :)^_^

	How to use
	==========
	$python nessDB-zip.py ndbs-path
	when finished, cover the old 'ndbs' folder files with new(files are in new_ndbs folder).
"""
__author__ = 'BohuTANG'

import sys
import os
import struct
import time

INT_SIZE = 4
LONG_SIZE = 8

"""
	Be careful with the KEY_SIZE, must same as config.h
"""
KEY_SIZE = 32

FOOTER_SIZE = 40
MAGIC = "2012"
DB_NAME = "ness.db"

class X:
	def __init__(self, oldpath, newpath):
		self.count = 0
		self.oldpath = oldpath
		self.newpath = newpath
		if(os.path.exists(newpath) != True):
			os.makedirs(newpath)

		self.old_db_fd = open(os.path.join(oldpath, DB_NAME), "r")
		self.new_db_fd = open(os.path.join(newpath, DB_NAME), "w")
		self.new_db_fd.write(MAGIC)
	
	def write_new_db(self, vlen, vdata):
		try:
			self.new_db_fd.write(vlen)
			self.new_db_fd.write(vdata)
			return self.new_db_fd.tell()
		except IOError as(errno, strerror):
			print "IO error ({0}):{1}".format(errno, strerror)
			

	def read_old_db(self, voff):
		try:
			self.old_db_fd.seek(voff, 0)
			vlen_be = self.old_db_fd.read(INT_SIZE)
			vlen = struct.unpack('>I', vlen_be)[0]
			vdata = self.old_db_fd.read(vlen)
			return self.write_new_db(vlen_be, vdata)
		except IOError as(errno, strerror):
			print "IO error ({0}):{1}".format(errno, strerror)

	def do_zip(self, sst_file):
		try:
			f = open(os.path.join(self.oldpath, sst_file), "r")
			new_f = open(os.path.join(self.newpath, sst_file), "w")

			f.seek(-FOOTER_SIZE, 2)
			footer_key = f.read(KEY_SIZE)
			footer_count = struct.unpack('>I', f.read(INT_SIZE))[0]
			footer_crc = struct.unpack('>I', f.read(INT_SIZE))[0]

			f.seek(0)
		
			for entry in range(0, footer_count):
				key = f.read(KEY_SIZE)
				voff = struct.unpack('>Q', f.read(LONG_SIZE))[0]
				new_voff = self.read_old_db(voff)
				new_voff = struct.pack('>Q', new_voff)
				new_f.write(key)
				new_f.write(new_voff)

				self.count += 1
				if (self.count % 10000) == 0:
					sys.stdout.write("\r\x1b[K ....finished " + self.count.__str__())
					sys.stdout.flush()

			#write footer
			new_f.write(footer_key)
			new_f.write(struct.pack('>I', footer_count))
			new_f.write(struct.pack('>I', footer_crc))

			f.close()
			new_f.close()
		except IOError as(errno, strerror):
			print "IO error ({0}):{1}".format(errno, strerror)



def main():
	if (len(sys.argv) > 1):
		oldpath = sys.argv[1]
		newpath = os.path.join(oldpath, "new_ndbs")
		x = X(oldpath, newpath)
		start_time = time.time()
		for sst in os.listdir(oldpath):
			if (sst.endswith(".sst")):
				x.do_zip(sst)

		end_time = time.time()
		print ""
		print "-------->count:<%i>,cost time:<%i>, %i/sec"  %(x.count, end_time - start_time, x.count/(end_time - start_time))
		print "-------->New files are in:%s" %x.newpath

	else:
	 print "nessDB-zip.py -<ndbs_path>"	

if __name__ == "__main__":
	main()

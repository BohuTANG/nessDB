#!/usr/bin/env python
#-*- coding:utf-8 -*-
# author : KDr2
#



import nessdb

db=nessdb.NessDB("test.db")
db.db_add("key","value")
print db.db_get("key")
print db.db_exists("key")
print db.db_exists("key1")
db.db_close()



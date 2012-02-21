#!/usr/bin/env python
#-*- coding:utf-8 -*-
# author : KDr2
#



import nessdb

db=nessdb.NessDB("test.db")
db.db_add("key","value")
print db.db_get("key")
db.db_close()



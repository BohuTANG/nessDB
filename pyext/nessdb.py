#!/usr/bin/env python
#-*- coding:utf-8 -*-
# author : KDr2
#


import _nessdb


class NessDB(object):


    def __init__(self,path,pool_size=1024*1024*512,tolog=0):
        self.db=_nessdb.db_open(path,pool_size,tolog)

    def db_add(self,key,value):
        return _nessdb.db_add(self.db,
                              key,len(key),
                              value,len(value))

    def db_get(self,key):
        return _nessdb.db_get(self.db,key,len(key))

    def db_remove(self,key):
        return _nessdb.db_remove(self.db,key,len(key))

    def db_close(self):
        return _nessdb.db_close(self.db)
    



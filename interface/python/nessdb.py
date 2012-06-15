#!/usr/bin/env python
#-*- coding:utf-8 -*-
# author : KDr2
#


import _nessdb


class NessDB(object):


    def __init__(self,path,is_log_recovery=1):
        self.db=_nessdb.db_open(path,is_log_recovery)

    def db_add(self,key,value):
        return _nessdb.db_add(self.db,
                              key,len(key),
                              value,len(value))

    def db_get(self,key):
        return _nessdb.db_get(self.db,key,len(key))

    def db_exists(self,key):
        return  _nessdb.db_exists(self.db,key,len(key))

    def db_remove(self,key):
        return _nessdb.db_remove(self.db,key,len(key))

    def db_info(self):
        return _nessdb.db_info(self.db)
   
    def db_close(self):
        return _nessdb.db_close(self.db)
    



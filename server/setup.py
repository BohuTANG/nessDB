#!/usr/bin/env python
#-*- coding:utf-8 -*-
# author : KDr2
#        : BohuTANG
#


import os
import sys
import glob

from distutils.core import setup, Extension

os.environ["CFLAGS"] = "-Wno-strict-prototypes -std=c99"

pyext_path=os.path.abspath(os.path.dirname(__file__))
nessdb_path=os.path.dirname(pyext_path)
cfiles=glob.glob(os.path.join(nessdb_path,"engine/*.c"))

ext_nessdb = Extension('_nessdb',
                       include_dirs = [os.path.join(nessdb_path,'engine')],
                       sources = cfiles+['nessdb_module.c'])

setup(name = 'nessdb',
      version = '0.1',
      description = 'nessDB python server',
      py_modules=['nessdb'],
      ext_modules = [ext_nessdb])




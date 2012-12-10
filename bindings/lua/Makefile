# Include path where lua.h, luaconf.h and lauxlib.h reside:
INCLUDES= -I/usr/local/include/luajit-2.0 -I../../engine

LDINCLUDES= -L../..
LD= -lnessdb

# Lua executable name. Used to find the install path and for testing.
LUA= luajit

CC= gcc
CCOPT= -O2 -fomit-frame-pointer
CCWARN= -Wall
SOCC= $(CC) -shared
SOCFLAGS= -fPIC $(CCOPT) $(CCWARN) $(INCLUDES) $(CFLAGS)
SOLDFLAGS= -fPIC $(LDFLAGS) $(LDINCLUDES)
RM= rm -f
INSTALL= install -p
INSTALLPATH= $(LUA) installpath.lua

MODNAME= nessdb
MODSO= $(MODNAME).so
MODLUA= $(MODNAME).lua

all: $(MODSO)

$(MODNAME).o: $(MODNAME).c
	$(CC) $(SOCFLAGS) -c -o $@ $<

$(MODSO): $(MODNAME).o
	$(SOCC) -o $@ $(SOLDFLAGS) $< $(LD)

install: $(MODSO)
	$(INSTALL) $< `$(INSTALLPATH) $(MODNAME)`
	$(INSTALL) $(MODLUA) `$(INSTALLPATH) $(MODNAME) ''`

test: $(MODSO)
	@sh -c 'export LD_LIBRARY_PATH=../../;$(LUA) test.lua && echo "test OK"'

clean:
	$(RM) *.o *.so *.obj *.lib *.exp *.dll *.manifest

.PHONY: all install test clean


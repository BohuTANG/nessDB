#!/usr/bin/env luajit
-- Tests for nessDB low level LuaJIT binding
-- Copyright (C) 2012 Olivier Goudron

local ffi = require("ffi")
local nessdb = require("nessdb")


local MAX_KEY_SIZE = 32 + 1 
local MAX_VALUE_SIZE = 255 + 1
local IS_LOG_RECOVERY = 1

local kbuf = ffi.new(nessdb.buffer_t, MAX_KEY_SIZE)
local vbuf = ffi.new(nessdb.buffer_t, MAX_VALUE_SIZE)
local sk = ffi.new(nessdb.slice_t)
local sv = ffi.new(nessdb.slice_t)
sk.data = kbuf
sv.data = vbuf

-- user friendly functions definitions
local function db_add(db, key, value)
	sk.len = string.len(key)
	ffi.copy(sk.data, key)
	sv.len = string.len(value)
	ffi.copy(sv.data, value)
	if nessdb.add(db, sk, sv) == 1 then return true else return false end
end

local function db_get(db, key)
	sk.len = string.len(key)
	ffi.copy(sk.data, key)
	if nessdb.get(db, sk, sv) == 1 then return ffi.string(sv.data) else return nil end
end

local function db_exists(db, key)
	sk.len = string.len(key)
	ffi.copy(sk.data, key)
	if nessdb.exists(db, sk) == 1 then return true else return false end
end

local function db_remove(db, key)
	sk.len = string.len(key)
	ffi.copy(sk.data, key)
	nessdb.remove(db, sk)
end

---- test code start here ----
local db = nessdb.open(MAX_KEY_SIZE * MAX_VALUE_SIZE * 1024, ".", IS_LOG_RECOVERY)

-- testing add
for n = 1,10 do
	db_add(db, "key" .. n, "value" .. n)
end

-- testing get
for n = 1,10 do
	print(db_get(db, "key" .. n))
end

-- testing exists
print(db_exists(db, "key5"))

-- testing remove
db_remove(db, "key5")

-- testing exists again
print(db_exists(db, "key5"))

-- db info and close
print(nessdb.info(db))
nessdb.close(db)

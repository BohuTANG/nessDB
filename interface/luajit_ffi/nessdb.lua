#!/usr/bin/env luajit
-- Quick and dirty low level nessDB LuaJIT FFI binding
-- The main use is for quick nessDB testing, a real binding should be more user friendly
-- Copyright (C) 2012 Olivier Goudron

local ffi = require("ffi")
ffi.cdef[[
struct slice{
   char *data;
   int len;
};
struct nessdb *db_open(size_t bufferpool_size, const char *basedir, int tolog);
int db_get(struct nessdb *db, struct slice *sk, struct slice *sv);
int db_exists(struct nessdb *db, struct slice *sk);
int db_add(struct nessdb *db, struct slice *sk, struct slice *sv);
void db_remove(struct nessdb *db, struct slice *sk);
char *db_info(struct nessdb *db);
void db_close(struct nessdb *db);
]]

local nessdb = ffi.load("../../libnessdb.so") -- libnessdb.so library path
local kbuf = ffi.new("char[33]") -- max key size in bytes + 1
local vbuf = ffi.new("char[1025]") -- max value size in bytes + 1
local sk = ffi.new("struct slice")
local sv = ffi.new("struct slice")
sk.data = kbuf
sv.data = vbuf

local function print_buf(message)
	print(message .. ffi.string(sk.data) .. " / " .. ffi.string(sv.data))
end

local function db_open(path)
	return nessdb.db_open(10580, path, 1) -- buffer pool size = (max_key_size + max_value_size) * number of LRU slots wanted
end

local function db_add(db, key, value)
	sk.len = string.len(key)
	ffi.copy(sk.data, key)
	sv.len = string.len(value)
	ffi.copy(sv.data, value)
	return nessdb.db_add(db, sk, sv)
end

local function db_get(db, key)
	sk.len = string.len(key)
	ffi.copy(sk.data, key)
	return nessdb.db_get(db, sk, sv)
end

local function db_exists(db, key)
	sk.len = string.len(key)
	ffi.copy(sk.data, key)
	return nessdb.db_exists(db, sk)
end

local function db_remove(db, key)
	sk.len = string.len(key)
	ffi.copy(sk.data, key)
	nessdb.db_remove(db, sk)
end

local function db_info(db)
	return ffi.string(nessdb.db_info(db))
end

local function db_close(db)
	nessdb.db_close(db)
end


---- test code start here ----
db = db_open(".")

-- testing add
for n = 1,10 do
	local exit_code = db_add(db, "key" .. n, "value" .. n)
	print("Function add : " .. ffi.string(sk.data) .. " / " .. ffi.string(sv.data) .. " > exit_code = " .. exit_code)
end

-- testing get
for n = 1,10 do
	local exit_code = db_get(db, "key" .. n)
	print("Function get : " .. ffi.string(sk.data) .. " / " .. ffi.string(sv.data) .. " > exit_code = " .. exit_code)
end

-- testing exists
for n = 1,10 do
	local exit_code = db_exists(db, "key" .. n)
	print("Function exists : " .. ffi.string(sk.data) .. " > exit_code = " .. exit_code)
end

-- testing remove
for n = 1,10 do
	db_remove(db, "key" .. n)
	print("Function remove : " .. ffi.string(sk.data))
end

-- testing exists again
for n = 1,10 do
	local exit_code = db_exists(db, "key" .. n)
	print("Function exists : " .. ffi.string(sk.data) .. " > exit_code = " .. exit_code)
end

-- db info and close
print(db_info(db))
db_close(db)

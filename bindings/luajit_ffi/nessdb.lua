-- Low level nessDB LuaJIT FFI binding
-- This binding export only nessDB functions and types
-- Copyright (C) 2012 Olivier Goudron

local ffi = require("ffi")

ffi.cdef[[
struct slice{
   char *data;
   int len;
};
struct nessdb *db_open(const char *basedir);
int db_get(struct nessdb *db, struct slice *sk, struct slice *sv);
int db_exists(struct nessdb *db, struct slice *sk);
int db_add(struct nessdb *db, struct slice *sk, struct slice *sv);
void db_remove(struct nessdb *db, struct slice *sk);
void db_stats(struct nessdb *db, struct slice *stats);
void db_close(struct nessdb *db);
]]

local N = {} -- exported functions and types

local nessdb_n = ffi.load("../../libnessdb.so") -- libnessdb.so namespace
N.buffer_t = ffi.typeof("char[?]") -- char buffer type
N.slice_t = ffi.typeof("struct slice") -- struct slice type


function N.openlib(lib_path)
	nessdb_n = ffi.load(lib_path)
end

-- base_dir = name of db directory
-- is_log_recovery = allow log recovery or not
function N.open(base_dir)
	return nessdb_n.db_open(base_dir)
end

function N.get(db, sk, sv)
	return nessdb_n.db_get(db, sk, sv)
end

function N.exists(db, sk)
	return nessdb_n.db_exists(db, sk)
end

function N.add(db, sk, sv)
	return nessdb_n.db_add(db, sk, sv)
end

function N.remove(db, sk)
	nessdb_n.db_remove(db, sk)
end

function N.stats(db, stats)
	return ffi.string(nessdb_n.db_stats(db, stats))
end

function N.close(db)
	nessdb_n.db_close(db)
end

return N

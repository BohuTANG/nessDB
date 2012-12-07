local nessdb = require "nessdb"

local path = "/tmp/test"
local key = "key1"
local value = "value1"

local db = nessdb:open(path)
assert(db ~= nil, "db open failed")

local r = db:set(key, value)
assert(r == true, "db add failed")

local v = db:get(key)
assert(v == value, "db get value failed")

local r = db:exists(key)
assert(r == true, "db exists failed")

db:remove(key)

local v = db:get(key)
assert(v == nil, "db must get nil, after remove")

local s = db:stats()
print(s)

db:close()


--[[
--C style
local dopen, dclose = nessdb.open, nessdb.close
local dget, dadd, dexist, ddel = nessdb.get, nessdb.add, nessdb.exists, nessdb.remove
local dstat = nessdb.stats

local path = "/tmp/test"
local key = "key1"
local value = "value1"

local db = dopen("/tmp/test")
assert(db ~= nil, "db open failed")

local r = dadd(db, key, value)
assert(r == true, "db add failed")

local v = dget(db, key)
assert(v == value, "db get value failed")

local r = dexist(db, key)
assert(r == true, "db exists failed")

ddel(db, key)
local v = dget(db, key)
assert(v == nil, "db must get nil, after remove")

local s = dstat(db)
print(s)

dclose(db)
]]

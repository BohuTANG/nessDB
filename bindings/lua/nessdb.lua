--[[

Nessdb lua binding
(c) 2012 tiger.yang <believe3301@gmail.com>

]]--

local nessdbc = require "nessdb.c"

local dopen, dclose = nessdbc.open, nessdbc.close
local dget, dadd, dexist, ddel = nessdbc.get, nessdbc.add, nessdbc.exists, nessdbc.remove
local dstat = nessdbc.stats

local getmetatable, setmetatable = getmetatable, setmetatable

module(...)

local mt = { __index = _M }

function open(self, bpath) 
    local db = dopen(bpath)

    if not db then
        return nil, "open db failed"
    end

    return setmetatable({ db = db }, mt);
end

function close(self)
    local db = self.db

    if not db then
        return false, "open db first"
    end

     dclose(db)
     self.db = nil
     return true
end

function get(self, key)
    local db = self.db

    if not db then
        return nil, "open db first"
    end

    local value = dget(db, key)
    return value, nil
end

function set(self, key, value)
    local db = self.db

    if not db then
        return nil, "open db first"
    end

    local r = dadd(db, key, value)
    return r
end

function remove(self, key)
    local db = self.db

    if not db then
        return nil, "open db first"
    end
    ddel(db, key)
    return true
end

function exists(self, key)
    local db = self.db

    if not db then
        return nil, "open db first"
    end
    local r = dexist(db, key)
    return r
end

function stats(self)
    local db = self.db

    if not db then
        return nil, "open db first"
    end
    
    local r = dstat(db)
    return r
end

local class_mt = {
    -- to prevent use of casual module global variables
    __newindex = function (table, key, val)
        error('attempt to write to undeclared variable "' .. key .. '"')
    end
}

setmetatable(_M, class_mt)


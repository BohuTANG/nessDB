--[[

Nessdb lua binding
(c) 2012 tiger.yang <believe3301@gmail.com>

]]--

local nessdb = require "nessdb"
local io = require "io"

local tostring = tostring
local unpack, type = unpack, type

local dget, ddel, dset, dexist = nessdb.get, nessdb.remove, nessdb.set, nessdb.exists

module(...)

local db = nil

-- get value
local function read_value(ngx) 
    ngx.req.read_body()
    local value = ngx.req.get_body_data()
    return value
end

-- method map table
local tmethod = {
    GET =    { dget },
    DELETE = { ddel },
    PUT =    { dset , read_value},
    POST =   { dset , read_value},
    HEAD =   { dexist }
}

-- for debug
local function perror(obj)
    local str = (obj and tostring(obj)) or 'nil'
	return io.stderr:write(str .. "\n")
end

-- rest api handle
-- @return code, err. if operate failed, return 400. 
function rest_handle(ngx)
    local err
    if not db then
        db, err = nessdb:open(ngx.var.db_path)
    end

    if not db then
        return 500, err
    end

    local mname = ngx.req.get_method()
    perror(mname)
    local mt = tmethod[mname]

    if not mt then
        return 405, "method not allowed"
    end

    local key = ngx.var.key
    local value

    if mt[2] then
        value = mt[2](ngx)
    end

    local ret, err = mt[1](db, unpack({key, value}))
    --return string
    if type(ret) == "string" and not err then
        return 200, ret
    end

    --return boolean
    if ret then 
        return 200, err
    else 
        return 400, "operate failed"
    end
end


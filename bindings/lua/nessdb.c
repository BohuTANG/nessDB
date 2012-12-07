/*
 *   Nessdb lua binding
 *   (c) 2012 tiger.yang <believe3301@gmail.com>
 */


#define LUA_LIB
#include "lua.h"
#include "lauxlib.h"
#include "stdio.h"
#include "db.h"

static inline void * 
checkuserdata(lua_State *L, int index)
{
    void *ud = lua_touserdata(L,index);
    if (ud == NULL) {
        luaL_error(L, "userdata %d is nil",index);
    }
    return ud;
}

static int nessdb_open(lua_State *L)
{
    const char *dirpath;
    dirpath = luaL_checkstring(L, 1);

    struct nessdb *db;
    db = db_open(dirpath);

    if (NULL == db) {
        return luaL_error(L, "open nessdb failed, basedir is %s",dirpath);
    }

	lua_pushlightuserdata(L, db);
	return 1;
}

static int nessdb_get(lua_State *L)
{
    struct nessdb *db;
    size_t sz = 0;
    const char *buffer;
    struct slice key, value;

    db = checkuserdata(L, 1);
    buffer = luaL_checklstring(L, 2 , &sz);

    key.data = (char *)buffer;
    key.len = sz;

    int ret = db_get(db, &key, &value);
    if (ret == 1) {
        lua_pushlstring(L,value.data,value.len);
        free(value.data);
        return 1;
    }

    return 0;
}

static int nessdb_add(lua_State *L)
{
    struct nessdb *db;
	size_t sz = 0;
	const char *buffer;
	struct slice key, value;
    
    db = checkuserdata(L, 1);
    buffer = luaL_checklstring(L, 2 , &sz);
    key.data = (char *)buffer;
    key.len = sz;

    buffer = luaL_checklstring(L, 3 , &sz);
    value.data = (char *)buffer;
    value.len = sz;

    int ret = db_add(db, &key, &value);

    lua_pushboolean(L, ret);
    return 1;
}

static int nessdb_remove(lua_State *L)
{
    struct nessdb *db;
	size_t sz = 0;
	const char *buffer;
	struct slice key;
    
    db = checkuserdata(L, 1);
    buffer = luaL_checklstring(L, 2 , &sz);
    key.data = (char *)buffer;
    key.len = sz;

    db_remove(db, &key);

    return 0;
}


static int nessdb_exists(lua_State *L)
{
    struct nessdb *db;
	size_t sz = 0;
	const char *buffer;
	struct slice key;
    
    db = checkuserdata(L, 1);
    buffer = luaL_checklstring(L, 2 , &sz);
    key.data = (char *)buffer;
    key.len = sz;

    int ret = db_exists(db, &key);

    lua_pushboolean(L, ret);
    return 1;
}

static int nessdb_stats(lua_State *L)
{
    struct nessdb *db;
    int blen = 512;
    char b[blen];
    struct slice stat = { b, blen};

    db = checkuserdata(L, 1);
    db_stats(db, &stat);
    lua_pushstring(L, stat.data);
    return 1;
}

static int nessdb_close(lua_State *L)
{
    struct nessdb *db;

    db = checkuserdata(L, 1);
    db_close(db);

    return 0;
}

static const struct luaL_Reg nessdb_funcs[] = 
{
    { "open",	nessdb_open },
    { "get",    nessdb_get },
    { "add",	nessdb_add },
    { "remove",	nessdb_remove },
    { "exists",	nessdb_exists },
    { "stats",	nessdb_stats },
    { "close",	nessdb_close },
    { NULL,     NULL }
};

LUALIB_API int luaopen_nessdb_c(lua_State *L)
{
#if LUA_VERSION_NUM < 502
    luaL_register(L, "nessdb.c", nessdb_funcs);
#else
    luaL_newlib(L, nessdb_funcs);
#endif
    return 1;
}

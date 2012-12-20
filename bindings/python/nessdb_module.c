/*
 * nessdb_module.c
 * author : KDr2
 *
 */


#include "db.h"
#include "Python.h"

static PyObject *
pynessdb_db_open(PyObject *self, PyObject *args)
{
    const char *db_path;
	struct nessdb *ret;
    
    if (!PyArg_ParseTuple(args, "s", &db_path))
        return NULL;

    ret = db_open(db_path);
    return Py_BuildValue("l", ret);
}

static PyObject *
pynessdb_db_add(PyObject *self, PyObject *args)
{
    struct slice sk, sv;
    long db;
    int ret;
    
    if (!PyArg_ParseTuple(args, "lsisi", &db, &sk.data, &sk.len, &sv.data, &sv.len))
        return NULL;
    ret=db_add((struct nessdb*)db, &sk, &sv);
    return Py_BuildValue("i", ret);
}

static PyObject *
pynessdb_db_remove(PyObject *self, PyObject *args)
{
    struct slice sk;
    long db;
    
    if (!PyArg_ParseTuple(args, "lsi", &db,&sk.data,&sk.len))
        return NULL;
    db_remove((struct nessdb*)db, &sk);
    Py_INCREF(Py_None);
    return Py_None;
}

static PyObject *
pynessdb_db_get(PyObject *self, PyObject *args)
{
    struct slice sk, sv;
    long db;
    int ret;
    
    if (!PyArg_ParseTuple(args, "lsi", &db,&sk.data,&sk.len))
        return NULL;
    ret = db_get((struct nessdb*)db, &sk, &sv);
    if (ret == 1) {
        PyObject *value=PyString_FromStringAndSize(sv.data,sv.len);
        db_free_data(sv.data);
        return value;
    }
    Py_INCREF(Py_None);
    return Py_None;
}

static PyObject *
pynessdb_db_exists(PyObject *self, PyObject *args)
{
    long db;
    int ret;
    struct slice sk;

    if (!PyArg_ParseTuple(args, "lsi", &db,&sk.data,&sk.len))
        return NULL;
    ret = db_exists((struct nessdb*)db, &sk);
    if (ret==1) { 
        Py_INCREF(Py_True);
        return Py_True;
    } else {
        Py_INCREF(Py_False);
        return Py_False;
    }
}

static PyObject *
pynessdb_db_stats(PyObject *self, PyObject *args)
{
    long db;
    char* stats;
    
    if (!PyArg_ParseTuple(args, "ls", &db, stats))
        return NULL;

    db_stats((struct nessdb*)db, stats);
    return Py_BuildValue("s", stats);
}


static PyObject *
pynessdb_db_close(PyObject *self, PyObject *args)
{
    long db=0;
    
    if (!PyArg_ParseTuple(args, "l", &db))
        return NULL;
    if(!db)
        db_close((struct nessdb*)db);
    Py_INCREF(Py_None);
    return Py_None;
}

static PyMethodDef NessDBMethods[] = {
    {"db_open",  pynessdb_db_open, METH_VARARGS,""},
    {"db_add",  pynessdb_db_add, METH_VARARGS,""},
    {"db_remove",  pynessdb_db_remove, METH_VARARGS,""},
    {"db_get",  pynessdb_db_get, METH_VARARGS,""},
    {"db_exists",  pynessdb_db_exists, METH_VARARGS,""},
    {"db_stats", pynessdb_db_stats, METH_VARARGS,""},
    {"db_close",  pynessdb_db_close, METH_VARARGS,""},
    {NULL, NULL, 0, NULL}
};

PyMODINIT_FUNC
init_nessdb(void)
{
    (void) Py_InitModule("_nessdb", NessDBMethods);
}

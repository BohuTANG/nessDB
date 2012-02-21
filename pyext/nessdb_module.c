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
    int bufferpool_size;
    const char *db_path;
    int tolog;
    
    if (!PyArg_ParseTuple(args, "sii", &db_path,&bufferpool_size,&tolog))
        return NULL;
    struct nessdb *ret = db_open(bufferpool_size,db_path,tolog);
    return Py_BuildValue("l", ret);
}

static PyObject *
pynessdb_db_add(PyObject *self, PyObject *args)
{
    struct slice sk, sv;
    long db;
    char *sk_data=NULL;
    int sk_len;
    char *sv_data=NULL;
    int sv_len;
    int ret;
    
    if (!PyArg_ParseTuple(args, "lsisi", &db, &sk_data, &sk_len, &sv_data, &sv_len))
        return NULL;
    sk.data=sk_data;
    sk.len=sk_len;
    sv.data=sv_data;
    sv.len=sv_len;
    ret=db_add((struct nessdb*)db, &sk, &sv);
    return Py_BuildValue("i", ret);
}


static PyObject *
pynessdb_db_remove(PyObject *self, PyObject *args)
{
    struct slice sk;
    long db;
    char *sk_data;
    int sk_len;
    
    if (!PyArg_ParseTuple(args, "lsi", &db,&sk_data,&sk_len))
        return NULL;
    sk.data=sk_data;
    sk.len=sk_len;
    db_remove((struct nessdb*)db, &sk);
    Py_INCREF(Py_None);
    return Py_None;
}

static PyObject *
pynessdb_db_get(PyObject *self, PyObject *args)
{
    struct slice sk, sv;
    long db;
    char *sk_data;
    int sk_len;
    int ret;
    
    if (!PyArg_ParseTuple(args, "lsi", &db,&sk_data,&sk_len))
        return NULL;
    sk.data=sk_data;
    sk.len=sk_len;
    ret = db_get((struct nessdb*)db, &sk, &sv);
    if (ret == 1) {
        PyObject *value=PyString_FromStringAndSize(sv.data,sv.len);
        free(sv.data);
        return value;
    }
    Py_INCREF(Py_None);
    return Py_None;
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
    {"db_close",  pynessdb_db_close, METH_VARARGS,""},
    {NULL, NULL, 0, NULL}
};

PyMODINIT_FUNC
init_nessdb(void)
{
    (void) Py_InitModule("_nessdb", NessDBMethods);
}




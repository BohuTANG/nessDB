#!/usr/bin/env python
#author: BohuTANG

import sys
import errno
import socket
import nessdb
import functools
from packet import packet
from tornado import ioloop, iostream

NONBLOCKING = (errno.EAGAIN, errno.EWOULDBLOCK)

def cli_un(sock, ndb, msg):
    sock.send(msg.packed(500))

def cli_set(sock, ndb, msg):
    if (len(msg.argv) == 2):
        if ndb.db_add(msg.argv[0][1], msg.argv[1][1]) == 1:
            sock.send(msg.packed(200))
    
    sock.send(msg.packed(500))

def cli_get(sock, ndb, msg):
    val = ndb.db_get(msg.argv[0][1])
    if val is not None:
        sock.send(msg.packed(201, [val]))
    else:
        sock.send(msg.packed(404))

def cli_info(sock, ndb, msg):
    val = ndb.db_info()
    sock.send(msg.packed(201,[val]))

def cli_ping(sock, ndb, msg):
    sock.send(msg.packed(202))

def cli_mset(sock, ndb, msg):
    argc = len(msg.argv)
    for i in xrange(0, argc - 1):
        if ndb.db_add(msg.argv[i][1], msg.argv[i + 1][1]) != 1:
            sock.send(msg.packed(500))
            return

    
    sock.send(msg.packed(200))

def cli_mget(sock, ndb, msg):
    vals = []
    argc = len(msg.argv)
    for i in xrange(0, argc):
        vals.append(ndb.db_get(msg.argv[i][1]))

    sock.send(msg.packed(201, vals))

def cli_exists(sock, ndb, msg):
    val = ndb.db_exists(msg.argv[0][1])
    if val == 1:
        sock.send(msg.packed(200))
    else:
        sock.send(msg.packed(404))

def cli_shutdown(sock, ndb, msg):
    ndb.db_close()
    sys.exit(1)

def cli_del(sock, ndb, msg):
    ndb.db_remove(msg.argv[0][1])
    sock.send(msg.packed(200))

options = {0 : cli_un,
        1 : cli_set,
        2 : cli_get,
        3 : cli_info,
        4 : cli_ping,
        5 : cli_mset,
        6 : cli_mget,
        7 : cli_exists,
        8 : cli_shutdown,
        9 : cli_del 
        }

def req_handle(sock, ndb, msg):
    buf = [""]
    wait_write = [0]

    def close():
        io_loop.remove_handler(sock.fileno())
        sock.close()

    def handle(fd, events):
        _buf = buf[0]
        try:
            _buf += sock.recv(1024*4)
            if not _buf:
                return close()

            msg.parse(_buf)
            #msg.dump()
            options[msg.cmd](sock, ndb, msg)
            msg.clean()
        except socket.error as e:
            if e.args[0] not in NONBLOCKING:
                print e
                return close()

    io_loop.add_handler(sock.fileno(), handle, io_loop.READ)

def conn_handle(sock, fd, events):
    global ndb
    msg = packet()
    while True:
        try:
            connection, address = sock.accept()
        except socket.error, e:
            if e[0] not in (errno.EWOULDBLOCK, errno.EAGAIN):
                raise
            return
        connection.setblocking(0)
        req_handle(connection, ndb, msg)

global ndb 
if __name__ == '__main__':
    ndb = nessdb.NessDB('.')
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.setblocking(0)
    sock.bind(("", 6379))
    sock.listen(1024)

    io_loop = ioloop.IOLoop.instance()
    callback = functools.partial(conn_handle, sock)
    io_loop.add_handler(sock.fileno(), callback, io_loop.READ)
    try:
        io_loop.start()
    except KeyboardInterrupt:
        io_loop.stop()
        print "exited cleanly"

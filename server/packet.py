#!/usr/bin/python
#author: BohuTANG

class cmd:
    UNKNOW = 0
    SET = 1
    GET = 2
    INFO = 3
    PING = 4
    MSET = 5
    MGET = 6
    EXISTS = 7
    SHUTDOWN = 8
    DEL = 9

class packet:
    def __init__(self):
        self.cmd = cmd.UNKNOW
        self.cmds = {'SET':cmd.SET, 'GET':cmd.GET, 
                        'INFO':cmd.INFO, 'PING':cmd.PING, 
                        'MSET':cmd.MSET, 'MGET':cmd.MGET, 'DEL':cmd.DEL,
                        'EXISTS':cmd.EXISTS, 'SHUTDOWN':cmd.SHUTDOWN}
        self.argc = 0
        self.argv = []

    def parse(self, request):
        if not request:
            return 0

        byte ,self.argc = request[0], int(request[1])
        request = request.split()
        cmd_key = request[2].upper()
        if self.cmds.has_key(cmd_key):
            self.cmd = self.cmds[cmd_key]

        k = 3
        for i in xrange(0, self.argc - 1):
            l = int(request[k][1:])
            val = request[k+1]
            if l != len(val):
                return -1;
            self.argv.append([l,val])
            k += 2
        return 1

    def clean(self):
        self.cmd = cmd.UNKNOW
        self.argc = 0
        self.argv = []

    def packed(self, status, argvs = None):
        if status == 200:
            return '+OK\r\n'

        elif status == 201:
            buf = '*%d\r\n' %(len(argvs))
            for x in argvs:
                line = '$%d\r\n%s\r\n' %(len(x), x)
                buf += line
            return buf
        elif status == 202:
            return '+PONG\r\n'

        elif status == 404:
            return '$-1\r\n'
        
        elif status == 500:
            return '-ERR\r\n'
        
        return '-No...\r\n'

    def dump(self):
        print 'request dump:'
        print '\tcmd:%s' %(self.cmd)
        for i in xrange(0,len(self.argv)):
            print '\t%d:%s' %(self.argv[i][0], self.argv[i][1])

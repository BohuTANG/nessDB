#ifndef _REQUEST_H
#define _REQUEST_H

#define REQ_MAX_BUFFSIZE (10240)

typedef enum {CMD_PING, 
	CMD_GET, 
	CMD_MGET,
	CMD_SET, 
	CMD_MSET, 
	CMD_DEL, 
	CMD_INFO, 
	CMD_EXISTS, 
	CMD_SHUTDOWN, 
	CMD_UNKNOW} CMD;

struct  cmds {
	char method[256];
	CMD cmd;
};

struct request {
	char querybuf[REQ_MAX_BUFFSIZE];
	int argc;
	char **argv;
	int pos;
	int multilen;
	int lastlen;
	int len;
	int idx;
	CMD cmd;
};

struct request *request_new();
int  request_parse(struct request *request);
int request_append(struct request *request, const char *buf, int n);
void request_clean(struct request *request);
void request_free(struct request *request);
void request_dump(struct request *request);

#endif

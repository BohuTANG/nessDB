#ifndef _REQUEST_H
#define _REQUEST_H

typedef enum{CMD_PING, CMD_GET, CMD_MGET, CMD_SET, CMD_MSET, CMD_DEL, CMD_INFO, CMD_EXISTS, CMD_SHUTDOWN, CMD_UNKNOW}CMD;

struct  cmds{
	char method[256];
	CMD cmd;
};

struct request{
	char *querybuf;
	int argc;
	char **argv;
	size_t pos;
	CMD cmd;
};

struct request *request_new(char *querybuf);
int  request_parse(struct request *request);
void request_free(struct request *request);
void request_dump(struct request *request);

#endif

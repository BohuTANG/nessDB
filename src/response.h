#ifndef _RESPONSE_H
#define _RESPONSE_H

/*OK:"+OK"
 *OK_200: without "+OK"
 *OK_404:"$-1"
 *ERR:"-OK"
 */
typedef enum{OK,OK_200,OK_404,OK_PONG,ERR}STATUS;

struct response{
	int argc;
	STATUS status;
	char *ackbuf;
	char **argv;
};

struct response *response_new(int argc,STATUS status);
void response_detch(struct response *response,char *ackbuf);
void response_dump(struct response *response);
void response_free(struct response *response);

#endif

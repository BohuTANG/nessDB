 /*Copyright (c) 2011-2012, BohuTANG <overred.shuttler at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of struct nessdb nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#ifndef __USE_GNU
#define __USE_GNU
#endif
#ifndef _XOPEN_SOURCE
#define _XOPEN_SOURCE
#endif


#include <sys/types.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <signal.h>

#include "anet.h"
#include "ae.h"
#include "request.h"
#include "response.h"
#include "../engine/util.h"
#include "../engine/debug.h"
#include "../engine/db.h"

char *_ascii_logo =
"\n=================================================\n"
"nessDB v1.8 with LSM-Tree ________________\n" 
"_____________________________  __ \\__  __ )\n"
"__  __ \\  _ \\_  ___/_  ___/_  / / /_  __  |\n"
"_  / / /  __/(__  )_(__  )_  /_/ /_  /_/ / \n"
"/_/ /_/\\___//____/ /____/ /_____/ /_____/  \n"
"=================================================\n\n";

#define BUF_SIZE (10240)
struct server{
	char *bindaddr;
	int port;
	int fd;

	aeEventLoop *el;
	char neterr[1024];
	struct nessdb *db;
};

struct server _svr;
static int _clicount;

void _process_cmd(int fd, struct request *req)
{
	char sent_buf[BUF_SIZE];
	struct response *resp;

	memset(sent_buf, 0, BUF_SIZE);
	/* request_dump(req);*/
	switch(req->cmd){
		case CMD_PING:{
						  resp=response_new(0,OK_PONG);
						  response_detch(resp,sent_buf);
						  write(fd,sent_buf,strlen(sent_buf));
						  response_free(resp);
						  break;
					  }

		case CMD_SET:{
						 struct slice sk, sv;

						 if(req->argc == 3) {
							 sk.len = strlen(req->argv[1]);
							 sk.data = req->argv[1];

							 sv.len = strlen(req->argv[2]);
							 sv.data = req->argv[2];

							 db_add(_svr.db, &sk, &sv);

							 resp=response_new(0,OK);
							 response_detch(resp,sent_buf);
							 write(fd,sent_buf,strlen(sent_buf));
							 response_free(resp);
							 break;
						 }
						 goto __default;
					 }
		case CMD_MSET:{
						  int i;
						  int c = req->argc;
						  for (i = 1; i < c; i += 2) {
							  struct slice sk, sv;

							  /* should do NULL detect */
							  sk.len = strlen(req->argv[i]);
							  sk.data = req->argv[i];

							  sv.len = strlen(req->argv[i+1]);
							  sv.data = req->argv[i+1];
							  db_add(_svr.db, &sk, &sv);
						  }

						  resp=response_new(0, OK);
						  response_detch(resp, sent_buf);
						  write(fd,sent_buf, strlen(sent_buf));
						  response_free(resp);
						  break;
					  }

		case CMD_GET:{
						 int ret;
						 struct slice sk;
						 struct slice sv;
						 if (req->argc == 2) {
							 sk.len=strlen(req->argv[1]);
							 sk.data = req->argv[1];
							 ret = db_get(_svr.db, &sk, &sv);
							 if (ret == 1) {
								 resp=response_new(1,OK_200);
								 resp->argv[0] = sv.data;
							 } else {
								 resp=response_new(0,OK_404);
								 resp->argv[0] = NULL;
							 }
							 response_detch(resp, sent_buf);
							 write(fd,sent_buf,strlen(sent_buf));
							 response_free(resp);
							 if (ret == 1)
								 free(sv.data);
							 break;
						 }
						 goto __default;
					 }
		case CMD_MGET:{
						  int i;
						  int ret;
						  int c=req->argc;
						  int sub_c=c-1;
						  char **vals = calloc(c, sizeof(char*));
						  resp=response_new(sub_c, OK_200);

						  for (i = 1; i < c; i++){
							  struct slice sk;
							  struct slice sv;
							  sk.len = strlen(req->argv[i]);
							  sk.data = req->argv[i];

							  ret = db_get(_svr.db, &sk, &sv);
							  if (ret == 1)
								  vals[i-1] = sv.data;
							  else
								  vals[i-1] = NULL;

							  resp->argv[i-1] = vals[i-1];
						  }

						  response_detch(resp, sent_buf);
						  write(fd, sent_buf, strlen(sent_buf));
						  response_free(resp);

						  for (i = 0; i < sub_c; i++){
							  if (vals[i])
								  free(vals[i]);	
						  }
						  free(vals);
						  break;
					  }
		case CMD_INFO:{
						  char *infos;	

						  infos = db_info(_svr.db);
						  resp = response_new(1, OK_200);
						  resp->argv[0] = infos;
						  response_detch(resp, sent_buf);
						  write(fd,sent_buf, strlen(sent_buf));
						  response_free(resp);
						  break;
					  }

		case CMD_DEL:{
						 int i;

						 for (i = 1; i < req->argc; i++){
							 struct slice sk;
							 sk.len = strlen(req->argv[i]);
							 sk.data = req->argv[i];
							 db_remove(_svr.db, &sk);
						 }

						 resp = response_new(0, OK);
						 response_detch(resp, sent_buf);
						 write(fd, sent_buf, strlen(sent_buf));
						 response_free(resp);
						 break;
					 }
		case CMD_EXISTS:{
							struct slice sk;
							sk.len = strlen(req->argv[1]);
							sk.data = req->argv[1];
							int ret= db_exists(_svr.db, &sk);
							if(ret)
								write(fd,":1\r\n",4);
							else
								write(fd,":-1\r\n",5);
						}
						break;

		case CMD_SHUTDOWN:
						__DEBUG(LEVEL_ERROR, "db-server shutdown...");
						db_close(_svr.db);
						exit(2);
						break;

__default:				default:{
									resp = response_new(0, ERR);
									response_detch(resp, sent_buf);
									write(fd, sent_buf, strlen(sent_buf));
									response_free(resp);
									break;
								}
	}

}


void read_handler(aeEventLoop *el, int fd, void *privdata, int mask)
{
	int err;
	int nread;
	(void) mask;
	char buf[BUF_SIZE]={0};
	struct request *req = (struct request*)privdata;

	nread = read(fd, buf, BUF_SIZE);

	if (nread < 1) {
		request_free(req);
		aeDeleteFileEvent(el, fd, AE_WRITABLE);
		aeDeleteFileEvent(el, fd, AE_READABLE);
		close(fd);
		_clicount--;
		return;
	} else {
		request_append(req, buf, nread);
		err = request_parse(req);
		if (err == -1) {
			request_free(req);
			aeDeleteFileEvent(el, fd, AE_WRITABLE);
			aeDeleteFileEvent(el, fd, AE_READABLE);
			close(fd);
			_clicount--;
			return;
		}

		/* got whole command */
		if (err == 1) {
			_process_cmd(fd, req);
			request_clean(req);
		}
	}
}

void accept_handler(aeEventLoop *el, int fd, void *privdata, int mask) 
{
	int cport, cfd;
	char cip[128];
	(void) el;
	(void)privdata;
	(void)mask;
	struct request *req;

	req = request_new();
	cfd = anetTcpAccept(_svr.neterr,fd,cip,&cport);
	if (cfd == AE_ERR)
		return;
	_clicount++;

	aeCreateFileEvent(_svr.el,cfd,AE_READABLE,read_handler,req);
}

int server_cron(struct aeEventLoop *eventLoop, long long id, void *clientData)
{
	(void) eventLoop;
	(void) id;
	(void) clientData;

	__DEBUG(LEVEL_WARNING, "%d clients connected", _clicount);
	return 3000;
}

#define BUFFERPOOL	(700*1024*1024)
#define IS_LOG_RECOVERY		(1)
struct nessdb *nessdb_open()
{
	return db_open(BUFFERPOOL, getcwd(NULL, 0), IS_LOG_RECOVERY);
}

void nessdb_close(struct nessdb *db)
{
	db_close(db);
}

#define HOST ("127.0.0.1")
#define PORT (6379)
int main(int argc, char **argv)
{
	(void) argc;
	(void) argv;

	_svr.bindaddr = HOST;
	_svr.port = PORT;

	_svr.db = nessdb_open();
	_svr.el = aeCreateEventLoop(11024);
	_svr.fd = anetTcpServer(_svr.neterr, _svr.port, _svr.bindaddr);
	if (_svr.fd == ANET_ERR) {
		__PANIC("openning port error #%d:%s", _svr.port, _svr.neterr);
		exit(1);
	}

	/*
	 * set nonblock: single process no effect
	 */
	if (anetNonBlock(_svr.neterr, _svr.fd) == ANET_ERR) {
		__DEBUG(LEVEL_ERROR, "set nonblock #%s",_svr.neterr);
		exit(1);
	}

	aeCreateTimeEvent(_svr.el, 3000, server_cron, NULL, NULL);

	/*handler*/
	if (aeCreateFileEvent(_svr.el, _svr.fd, AE_READABLE, accept_handler, NULL) == AE_ERR) 
		__DEBUG(LEVEL_ERROR, "creating file event");

	__DEBUG(LEVEL_INFO, "nessDB server starting, port:%d, pid:%d", PORT, (int)getpid());
	printf("%s", _ascii_logo);
	aeMain(_svr.el);
	__DEBUG(LEVEL_ERROR, "oops,exit");
	aeDeleteEventLoop(_svr.el);
	nessdb_close(_svr.db);
	return 1;
}

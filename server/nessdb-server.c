 /*Copyright (c) 2011, BohuTANG <overred.shuttler at gmail dot com>
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
 *   * Neither the name of nessDB nor the names of its contributors may be used
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


#if defined(__linux__) || defined(__APPLE__)
	# include <execinfo.h>
	# include <ucontext.h>
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
#include "../nessDB/db.h"
#include "../nessDB/platform.h"


struct server{
	char *bindaddr;
	int port;
	int fd;

	aeEventLoop *el;
	char neterr[1024];
	nessDB *db;
};

static void *get_mcontext_eip(ucontext_t *uc) {
#if defined(__FreeBSD__)
    return (void*) uc->uc_mcontext.mc_eip;
#elif defined(__dietlibc__)
    return (void*) uc->uc_mcontext.eip;
#elif defined(__APPLE__) && !defined(MAC_OS_X_VERSION_10_6)
  #if __x86_64__
    return (void*) uc->uc_mcontext->__ss.__rip;
  #elif __i386__
    return (void*) uc->uc_mcontext->__ss.__eip;
  #else
    return (void*) uc->uc_mcontext->__ss.__srr0;
  #endif
#elif defined(__APPLE__) && defined(MAC_OS_X_VERSION_10_6)
  #if defined(_STRUCT_X86_THREAD_STATE64) && !defined(__i386__)
    return (void*) uc->uc_mcontext->__ss.__rip;
  #else
    return (void*) uc->uc_mcontext->__ss.__eip;
  #endif
#elif defined(__i386__)
    return (void*) uc->uc_mcontext.gregs[14]; /* Linux 32 */
#elif defined(__X86_64__) || defined(__x86_64__)
    return (void*) uc->uc_mcontext.gregs[16]; /* Linux 64 */
#elif defined(__ia64__) /* Linux IA64 */
    return (void*) uc->uc_mcontext.sc_ip;
#else
    return NULL;
#endif
}

void back_trace(int sig_num, siginfo_t * info, void * ucontext)
{
 	 void *             array[50];
	 char **            messages;
	 int                size, i;
	 ucontext_t *   uc;

	 uc = (ucontext_t *)ucontext;

	 fprintf(stderr, "signal %d (%s), address is %p\n", 
	 sig_num, strsignal(sig_num), info->si_addr);

	 size = backtrace(array, 50);

	 /* overwrite sigaction with caller's address */
	if(get_mcontext_eip(uc) != NULL){
        	array[1] = get_mcontext_eip(uc);
   	 }
	 messages = backtrace_symbols(array, size);

	 /* skip first stack frame (points here) */
	 for (i = 1; i < size && messages != NULL; ++i)
	  	fprintf(stderr, "[bt]: (%d) %s\n", i, messages[i]);

	 free(messages);
	 exit(EXIT_FAILURE);
}

void signal_init()
{
	struct sigaction sigact;
	sigact.sa_sigaction = back_trace;
 	sigact.sa_flags = SA_RESTART | SA_SIGINFO;

 	if (sigaction(SIGSEGV, &sigact, (struct sigaction *)NULL) != 0){
  		fprintf(stderr, "error setting signal handler for %d (%s)\n",
    		SIGSEGV, strsignal(SIGSEGV));

  		exit(EXIT_FAILURE);
 	}

}


struct server _svr;
static int _clicount;

#define BUF_SIZE (10240)
void read_handler(aeEventLoop *el, int fd, void *privdata, int mask)
{
	char buf[BUF_SIZE]={0};
	int nread;

	nread=read(fd,buf,BUF_SIZE);
	if(nread==-1)
		return;

	if(nread==0){
		aeDeleteFileEvent(el, fd, AE_WRITABLE);
		aeDeleteFileEvent(el, fd, AE_READABLE);
		_clicount--;
		return;
	}else{
		int ret;
		struct request *req=request_new(buf);
		struct response *resp;
		ret=request_parse(req);
		if(ret==1){
			char sent_buf[BUF_SIZE]={0};
			request_dump(req);

			switch(req->cmd){
				case CMD_PING:{
						resp=response_new(0,OK_PONG);
						response_detch(resp,sent_buf);
						write(fd,sent_buf,strlen(sent_buf));
						response_dump(resp);
						response_free(resp);
						break;
					   }

				case CMD_SET:{
						db_add(_svr.db, req->argv[1], req->argv[2]);

						resp=response_new(0,OK);
						response_detch(resp,sent_buf);
						write(fd,sent_buf,strlen(sent_buf));
						response_dump(resp);
						response_free(resp);
						break;
					   }
				case CMD_MSET:{
					    int i;
						int c=req->argc;
						for(i=1;i<c;i+=2){
							db_add(_svr.db, req->argv[i], req->argv[i+1]);
						}

						resp=response_new(0,OK);
						response_detch(resp,sent_buf);
						write(fd,sent_buf,strlen(sent_buf));
						response_dump(resp);
						response_free(resp);
						break;
					      }

				case CMD_GET:{
						void* result;
						result=db_get(_svr.db, req->argv[1]);
						if(result==NULL)
							resp=response_new(0,OK_404);
						else{
							resp=response_new(1,OK_200);
							resp->argv[0]=result;
						}
						response_detch(resp,sent_buf);
						write(fd,sent_buf,strlen(sent_buf));
						response_dump(resp);
						response_free(resp);

						if(result)
							free(result);
						break;
					     }
				case CMD_MGET:{
						int c=req->argc;
						int sub_c=c-1;
						char **vals=calloc(c,sizeof(char*));
					        int i;
						resp=response_new(sub_c,OK_200);

						for(i=1;i<c;i++){
							vals[i-1]=db_get(_svr.db, req->argv[i]);
							resp->argv[i-1]=vals[i-1];
						}

						response_detch(resp,sent_buf);
						write(fd,sent_buf,strlen(sent_buf));
						response_dump(resp);
						response_free(resp);

						for(i=0;i<sub_c;i++){
							if(vals[i])
								free(vals[i]);	
						}
						free(vals);
					      	break;
					      }
				case CMD_INFO:{
						char infos[BUF_SIZE]={0};	
						db_info(_svr.db, infos);
						resp=response_new(1,OK_200);
					 	resp->argv[0]=infos;
						response_detch(resp,sent_buf);
						write(fd,sent_buf,strlen(sent_buf));
						response_dump(resp);
						response_free(resp);
						break;
					      }

				case CMD_DEL:{
						for(int i=1;i<req->argc;i++)
					     		db_remove(_svr.db, req->argv[i]);

						resp=response_new(0,OK);
						response_detch(resp,sent_buf);
						write(fd,sent_buf,strlen(sent_buf));
						response_dump(resp);
						response_free(resp);
						break;
					     }
				case CMD_EXISTS:{
						 int ret= db_exists(_svr.db, req->argv[1]);
						 if(ret)
							write(fd,":1\r\n",4);
						 else
							write(fd,":-1\r\n",5);
						}
						break;

				default:{
						resp=response_new(0,ERR);
						response_detch(resp,sent_buf);
						write(fd,sent_buf,strlen(sent_buf));
						response_dump(resp);
						response_free(resp);
						break;
					}
			}

		}

		request_free(req);
	}
}

void accept_handler(aeEventLoop *el, int fd, void *privdata, int mask) 
{
	int cport, cfd;
    	char cip[128];
   	cfd = anetTcpAccept(_svr.neterr,fd,cip,&cport);
	if (cfd == AE_ERR) {
		printf("accept....\n");
		return;
	}
	_clicount++;

	aeCreateFileEvent(_svr.el,cfd,AE_READABLE,read_handler,NULL);
}

int server_cron(struct aeEventLoop *eventLoop, long long id, void *clientData)
{
	printf("%d clients connected\n ",_clicount);
	return 3000;
}

#define BUFFERPOOL	(1024*1024*1024)
nessDB *nessdb_init()
{
	return db_init(BUFFERPOOL);
}

void nessdb_destroy(nessDB *db)
{
	db_destroy(db);
}

int main()
{
	signal_init();

	_svr.bindaddr="127.0.0.1";
	_svr.port=6379;
	
	_svr.db = nessdb_init();
	_svr.el=aeCreateEventLoop();
	_svr.fd=anetTcpServer(_svr.neterr,_svr.port,_svr.bindaddr);

	aeCreateTimeEvent(_svr.el, 3000, server_cron, NULL, NULL);

	/*handler*/
 	if (aeCreateFileEvent(_svr.el, _svr.fd, AE_READABLE,accept_handler, NULL) == AE_ERR) 
		printf("creating file event");

	aeMain(_svr.el);
	printf("oops,exit\n");
	aeDeleteEventLoop(_svr.el);
	nessdb_destroy(_svr.db);
	return 1;
}

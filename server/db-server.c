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
#include <stdbool.h>
#include <assert.h>
#include <err.h>
#include <getopt.h>
#include <inttypes.h>

#include "anet.h"
#include "ae.h"
#include "request.h"
#include "response.h"
#include "../engine/db.h"
#include "../engine/platform.h"
#include "../engine/util.h"


struct server{
	char *dbpath;
	char *bindaddr;
	int port;
	int fd;
	int client_count;

	aeEventLoop *el;
	char neterr[1024];
	struct nessdb *db;
};
typedef struct server server_t;


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
  		errx(EXIT_FAILURE, "error setting signal handler for %d (%s)\n",
    		SIGSEGV, strsignal(SIGSEGV));
 	}
}


#define BUF_SIZE (10240)
struct response* 
svr_handle(server_t *svr, struct request *req)
{
	/* TODO: response needs to free values retrieved from the DB */

	assert( req != NULL );
	switch(req->cmd){
		case CMD_PING:{
			return response_new(0,OK_PONG);
		}

		case CMD_SET:{
			struct slice k = {req->argv[1], strlen(req->argv[1]), 0};
			struct slice v = {req->argv[2], strlen(req->argv[2]), 0};
			db_add(svr->db, &k, &v);

			return response_new(0,OK);
		}

		case CMD_MSET:{
			int i;
			int c=req->argc;
			struct slice k, v;
			k.park = 0;
			v.park = 0;

			for(i=1;i<c;i+=2){
				k.data = req->argv[i]; k.len = strlen(req->argv[i+1]);
				v.data = req->argv[i + 1]; v.len = strlen(req->argv[i+1]);
				db_add(svr->db, &k, &v);				
			}

			return response_new(0, OK);
		}

		case CMD_GET:{
			struct slice k = {req->argv[1], strlen(req->argv[1]), 0};
			struct slice v;
			if( ! db_get(svr->db, &k, &v) ) {
				return response_new(0,OK_404);
			}
							
			struct response *resp=response_new(1,OK_200);
			resp->argv[0]=v.data;
			resp->to_free = calloc( 2, sizeof(void*) );
			assert( resp->to_free != NULL );
			resp->to_free[0] = v.data;
			resp->to_free[1] = NULL;
			return resp;
		}

		case CMD_MGET:{
			int c=req->argc;
			struct response *resp = response_new(c - 1,OK_200);
			resp->to_free = calloc(c, sizeof(void *));
			for(int i=1; i<c; i++){
				struct slice k = {req->argv[i], strlen(req->argv[i]), 0};
				struct slice v;

				// XXX: hack so response_free will clean all
				if( ! db_get(svr->db, &k, &v) ) {
					v.data = strdup("");
					v.len = 1;
				}

				resp->argv[i-1] = v.data;
				resp->to_free[i - 1] = v.data;
			}
			resp->to_free[c - 1] = NULL;

			return resp;
		}

		case CMD_INFO:{
			char* infos = db_info(svr->db);
			struct response* resp = response_new(1,OK_200);
			resp->argv[0]=infos;
			resp->to_free = calloc(2, sizeof(void*));
			resp->to_free[0] = infos;
			resp->to_free[1] = NULL;
			return resp;
		}

		case CMD_DEL:{
			struct slice k;
			for(int i=1;i<req->argc;i++) {
				k.data = req->argv[i];
				k.len = strlen(req->argv[i]);
				db_remove(svr->db, &k);
			}

			return response_new(0,OK);
		}

		case CMD_EXISTS:{
			struct slice k = {req->argv[1], strlen(req->argv[1]), 0};
			int ret= db_exists(svr->db, &k);
			return response_new(0, ret ? OK_TRUE : OK_FALSE);
		}					

		default:{
			return response_new(0,ERR);
		}
	}
	assert( 0 );
}


void svr_read_cb(aeEventLoop *el, int fd, void *_svr, int mask)
{
	server_t *svr = (server_t*)_svr;
	char buf[BUF_SIZE]={0};
	int nread;
	(void)el; (void)mask;

	nread=read(fd,buf,BUF_SIZE);
	if(nread==-1)
		return;

	if(nread==0){
		aeDeleteFileEvent(el, fd, AE_WRITABLE);
		aeDeleteFileEvent(el, fd, AE_READABLE);
		svr->client_count--;
		return;
	}

	struct request *req=request_new(buf);		
	if( request_parse(req) ){
		struct response *resp = NULL;
		char sent_buf[BUF_SIZE]={0};

		resp = svr_handle(svr, req);
		response_detch(resp, sent_buf);
		write(fd, sent_buf, strlen(sent_buf));
		response_free(resp);
	}
	request_free(req);
}


void svr_accept_cb(aeEventLoop *el, int fd, void *_svr, int mask) 
{
	server_t *svr = (server_t*)_svr;
	int cport, cfd;
    char cip[128];
    (void)el; (void)mask;

   	cfd = anetTcpAccept(svr->neterr,fd,cip,&cport);
	if (cfd == AE_ERR) {
		warnx("Could not accept: %s", svr->neterr);
		printf("accept....\n");
		return;
	}
	svr->client_count++;

	int x = aeCreateFileEvent(svr->el, cfd, AE_READABLE, svr_read_cb, svr);
	assert( x == AE_OK );
}


int svr_cron_cb(struct aeEventLoop *eventLoop, long long id, void *_svr)
{
	server_t *svr = (server_t*)_svr;
	(void)eventLoop;
	printf("%d clients connected (%lld)\n ", svr->client_count, id);
	return 3000;
}


#define BUFFERPOOL	(1024*1024*1024)
void svr_init(server_t *svr)
{
	memset(svr, 0, sizeof(server_t));

	// Sensible defaults
	svr->bindaddr="127.0.0.1";
	svr->port=6379;
}


void svr_run(server_t *svr)
{
	if( svr->dbpath == NULL ) {
		svr->dbpath = getcwd(NULL, 0);
	}

	svr->db = db_open(BUFFERPOOL, svr->dbpath);
	if( ! svr->db ) {
		errx(EXIT_FAILURE, "Failed to open database in '%s'", svr->dbpath);
	}

	svr->el = aeCreateEventLoop();
	assert( svr->el != NULL );

	long long cron_event = aeCreateTimeEvent(svr->el, 3000, svr_cron_cb, svr, NULL);
	assert( cron_event != AE_ERR );

	svr->fd = anetTcpServer(svr->neterr, svr->port, svr->bindaddr);
	if( svr->fd == AE_ERR ) {
		errx(EXIT_FAILURE, "Cannot start TCP server on %s:%d -- %s", svr->bindaddr, svr->port, svr->neterr);
	}

 	int file_event = aeCreateFileEvent(svr->el, svr->fd, AE_READABLE, svr_accept_cb, svr);
 	assert( file_event == AE_OK );

	aeMain(svr->el);
}

// TODO: svr_open and svr_close

void svr_destroy(server_t *svr)
{
	if( svr->el ) {
		aeDeleteEventLoop(svr->el);
	}

	if( svr->db ) {
		db_close(svr->db);
	}

	if( svr->dbpath ) {
		free(svr->dbpath);
	}
}


static void
show_help(char *prog) {
	fprintf(stderr,
		"Usage: %s [options]\n"
		"\t--help|-h             - Show usage instructions\n"
		"\t--listen|-l <address> - Bind address for Redis listener\n"
		"\t--dbpath|-d <dir>     - Directory to store database\n",
		prog);
} 

static void
parse_bind_addr( char *in, char **addr, int *port ) {
	char *ps = strrchr(in, ':');
	if( ps ) {
		int p;
		ps[0] = 0;
		p = atoi(++ps);
		if( p < 1 || p > 0xFFFF ) {
			warnx("Invalid port %d from bind address '%s', using default", p, in);
		}
		else {
			*port = atoi(ps);	
		}				
	}
	*addr = strdup(in);
}


static bool
parse_opts( int argc, char **argv, server_t *svr ) {
	while( true ) {
		static struct option long_options[] = {
			{"help", 0, 0, 'h'},
			{"listen", 1, 0, 'l'},
			{"dbpath", 1, 0, 'd'},
			{0, 0, 0, 0}
		};
		int idx = 0, c;

		c = getopt_long(argc, argv, "hl:d:", long_options, &idx);
		if( c == -1 ) break; 

		switch(c) {
		case 'l':
			parse_bind_addr(optarg, &svr->bindaddr, &svr->port);
			break;

		case 'd':
			svr->dbpath = optarg;
			break;

		case 'h':
		default:
			show_help(argv[0]);
			return false;
		}
	}
	return true;
}

int main(int argc, char **argv)
{
	server_t instance;

	signal_init();
	svr_init(&instance);

	if( parse_opts(argc, argv, &instance) ) {
		svr_run(&instance);
	}

	svr_destroy(&instance);
	return EXIT_SUCCESS;
}

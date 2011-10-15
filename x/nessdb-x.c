 /* Copyright (c) 2011, BohuTANG <overred.shuttler at gmail dot com>
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
 *   * Neither the name of Redis nor the names of its contributors may be used
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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <time.h>
#include <stdarg.h>

#include <sys/socket.h>
#include <sys/epoll.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <unistd.h>

#include "request.h"
#include "event.h"

#define BUF_SIZE (10240)


/********************redis style log,lol************************/
static void log_raw(const char *msg) {
   
    time_t now = time(NULL);

    char buf[64];

    strftime(buf,sizeof(buf),"%d %b %H:%M:%S",localtime(&now));
    fprintf(stderr,"[%d] %s  %s\n",(int)getpid(),buf,msg);
}

static void log(const char *fmt, ...){
    va_list ap;
    char msg[1024];

    va_start(ap, fmt);
    vsnprintf(msg, sizeof(msg), fmt, ap);
    va_end(ap);
     
    log_raw(msg);
}
/**************************************************************/

void read_cb(struct event *event,struct event_node *node,struct epoll_event ev)
{
	log_raw("data coming...");
	struct request *request;
	char req_buf[BUF_SIZE]={0};
	int r;
    	r=read(node->fd,req_buf,BUF_SIZE);
	if(r==0)
		return;

	request=request_new();

       	if(request){
		request->data=(req_buf);
      		request_parse(request);

		log("dump request:cmd:<%s>,key len:<%d>,key:<%s>,value length:<%d>,value:<%s>",
				request->cmd,
				request->k_len,
				request->k,
				request->v_len,
				request->v);

		if(strcmp(request->cmd,"SET")==0){
			//todo set
		}else if(strcmp(request->cmd,"GET")==0){
			//todo get
		}else if(strcmp(request->cmd,"UPDATE")==0){
			//todo update
		}else if(strcmp(request->cmd,"REMOVE")==0){
			//todo remove
		}else{
			log("unknow cmd:%s",request->cmd);
		}

      		request_free(request);
   	}
}

void close_cb(struct event * event, struct event_node *node, struct epoll_event ev)
{
    event_remove(event, node->fd);
    log("<%d> close ...",node->fd);
}

void accept_cb(struct event *event,struct event_node * node, struct epoll_event ev)
{
	int listen_fd;
	uint32_t flags;
    	struct sockaddr_in clt_addr;
	struct event_node *n;

    	socklen_t clt_len = sizeof(clt_addr);
	listen_fd = accept(node->fd, (struct sockaddr*) &clt_addr, &clt_len);
    	fcntl(listen_fd, F_SETFL, O_NONBLOCK);
    	log("accept_cb ,fd is:%d",listen_fd);
	
	flags = EPOLLIN | EPOLLRDHUP | EPOLLHUP;
	
	event_add(event, listen_fd, flags, &n);
    	n->read_callback = read_cb;
	n->close_callback = close_cb;
}

int timeout_cb(struct event *event)
{
	log("epoll check....");
	return 0;
}


int main()
{
	int sock,port,back_log,timeout;
	struct sockaddr_in svr_addr;
	struct event *ent;
	struct event_node *node;

	port=8080;
	back_log=10;
	timeout=1000;//1sec
	ent=calloc(1,sizeof(struct event));

    	sock = socket(AF_INET, SOCK_STREAM, 0);
    	memset(&svr_addr, 0 , sizeof(svr_addr));
    	svr_addr.sin_family = AF_INET;
    	svr_addr.sin_addr.s_addr = htons(INADDR_ANY);
    	svr_addr.sin_port = htons(port);
    	bind(sock, (struct sockaddr *) &svr_addr, sizeof(svr_addr));
    	listen(sock, back_log);
    	fcntl(sock, F_SETFL, O_NONBLOCK);

	//event init
	event_init(ent,timeout);
	ent->timeout_callback=timeout_cb;

	event_add(ent,sock,EPOLLIN,&node);
    	node->accept_callback = accept_cb;
    	node->close_callback = close_cb;
    
	// enable accept callback
	node->cb_flags |= ACCEPT_CB;

	event_loop(ent);

	return 1;
}

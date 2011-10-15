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

#include <sys/epoll.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include "../src/ht.h"
#include "event.h"

#define PRIME (1543)

static struct ht _ht;

void event_init(struct event *event,int timeout)
{
	event->timeout=timeout;
	event->epoll_fd=epoll_create(MAX_EVENTS);

	ht_init(&_ht,PRIME,INT);
}

int event_add(struct event *event,int fd,uint32_t flags,struct event_node **node)
{
	struct event_node *v;
	v=ht_get(&_ht,&fd);
	if(v){
		struct epoll_event ev;
		v->events|=flags;

		memset(&ev, 0, sizeof(struct epoll_event));
		ev.data.fd=fd;
		ev.events=v->events;
		*node=v;
		return epoll_ctl(event->epoll_fd, EPOLL_CTL_MOD, fd, &ev);
	}else{
		struct epoll_event ev;
		struct event_node *n;
		n=calloc(1,sizeof(struct event_node));
		n->events=flags;
		n->fd=fd;
		ht_set(&_ht,&fd,n);

        	memset(&ev, 0, sizeof(struct epoll_event));
        	ev.data.fd = fd;
       		ev.events = n->events;
        	*node = n;
        	return epoll_ctl(event->epoll_fd, EPOLL_CTL_ADD, fd, &ev);
	}
}

int event_remove(struct event *event,int fd)
{
	struct event_node *n;
	n=ht_get(&_ht,&fd);
	if(n){
		ht_remove(&_ht,&fd);
		close(fd);
    		epoll_ctl(event->epoll_fd, EPOLL_CTL_DEL, fd, NULL);
	}
	return 0;
}

int event_process(struct event *event)
{
	int fds,i;
	struct epoll_event events[MAX_EVENTS];
	
	fds = epoll_wait(event->epoll_fd, events, MAX_EVENTS,event->timeout);
	if(fds==0){
		if (event->timeout_callback)
			event->timeout_callback(event);
   		return 0;
	}

	for(i=0;i<fds;i++){
		int e_fd;
		struct event_node *n;

		e_fd=events[i].data.fd;
		n=ht_get(&_ht,&e_fd);
		if(n){
			 if ((events[i].events & EPOLLIN) || (events[i].events & EPOLLPRI)){
				 if (events[i].events & EPOLLIN)
                    			n->cur_event &= EPOLLIN;
               		 	 else
					n->cur_event &= EPOLLPRI;
			
		 		 if ((n->cb_flags & ACCEPT_CB) && (n->accept_callback))
	     			 n->accept_callback(event,n, events[i]);
		 		 if ((n->cb_flags & CONNECT_CB) && (n->connect_callback))
		     			 n->connect_callback(event, n, events[i]);
				 if (n->read_callback)
		     			 n->read_callback(event,n, events[i]);
			 }

			 if (events[i].events & EPOLLOUT){
		 		 n->cur_event &= EPOLLOUT;
				 if (n->write_callback)
		     			 n->write_callback(event, n, events[i]);
	     		 }

	     		 if ( (events[i].events & EPOLLRDHUP) || (events[i].events & EPOLLERR) || (events[i].events & EPOLLHUP)){
		 		 if (events[i].events & EPOLLRDHUP)
		     			 n->cur_event &= EPOLLRDHUP;
		 		 else
		     			 n->cur_event &= EPOLLERR;
			 	if (n->close_callback)
	     				 n->close_callback(event,n, events[i]);
			 }

		}else{
			printf("***ERROR*** not in events table\n");
		}
	}

	return 0;
}

void event_loop(struct event *event)
{
	int i;
	i=0;
	while(i==0){
		i=event_process(event);
	}
	printf("***ERROR*** event loop:%d\n",i);
}

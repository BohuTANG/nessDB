#ifndef _EVENT_H_
#define _EVENT_H_

#define MAX_EVENTS (99)
#define ACCEPT_CB (0x01)
#define CONNECT_CB (0x02)

#define CALLBACK(x) void (*x) (struct event *,struct event_node *, struct epoll_event)

struct event{
	int timeout;
	int epoll_fd;

	int (*timeout_callback)(struct event *);
};


struct event_node{
	int fd;
	uint32_t events;
	uint32_t cur_event;
	uint8_t cb_flags;
	void *data;

	//callbacks
	CALLBACK(write_callback);
	CALLBACK(read_callback);
        CALLBACK(close_callback);
        CALLBACK(accept_callback);
        CALLBACK(connect_callback);
};


void event_init(struct event *event,int timeout);
int event_add(struct event *event,int fd,uint32_t flags,struct event_node **node);
int event_remove(struct event *event,int fd);
int event_process(struct event *event);
void event_loop(struct event *event);

#endif

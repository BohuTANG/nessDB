/* Select()-based ae.c module
 * Copyright (C) 2009-2010 Salvatore Sanfilippo - antirez@gmail.com
 * Released under the BSD license. See the COPYING file for more info. */

#include <string.h>
#include "win32fixes.h"

typedef struct aeApiState {
    fd_set rfds, wfds;
    /* We need to have a copy of the fd sets as it's not safe to reuse
     * FD sets after select(). */
    fd_set _rfds, _wfds;
    /* WIN32  select works only on sockets, so we will wati for pipes  */
    HANDLE vm_pipe;
} aeApiState;

static int aeApiCreate(aeEventLoop *eventLoop) {
    aeApiState *state = zmalloc(sizeof(aeApiState));
	
    if (!state) return -1;
	
    FD_ZERO(&state->rfds);
    FD_ZERO(&state->wfds);
    state->vm_pipe = INVALID_HANDLE_VALUE;
	
    eventLoop->apidata = state;
    return 0;
}

static void aeApiFree(aeEventLoop *eventLoop) {
    zfree(eventLoop->apidata);
}

static int aeApiAddEvent(aeEventLoop *eventLoop, int fd, int mask) {
    aeApiState *state = eventLoop->apidata;
	
    if (mask & AE_PIPE) {
        state->vm_pipe = (HANDLE) _get_osfhandle(fd);
		//        DWORD mode = PIPE_NOWAIT;
		//        SetNamedPipeHandleState(state->vm_pipe,&mode,NULL,NULL);
    } else {
        if (mask & AE_READABLE) FD_SET((SOCKET)fd,&state->rfds);
        if (mask & AE_WRITABLE) FD_SET((SOCKET)fd,&state->wfds);
    }
    return 0;
}

static void aeApiDelEvent(aeEventLoop *eventLoop, int fd, int mask) {
    aeApiState *state = eventLoop->apidata;
	
    if (mask & AE_PIPE) {
        state->vm_pipe = INVALID_HANDLE_VALUE;
    } else {
        if (mask & AE_READABLE) FD_CLR((SOCKET)fd,&state->rfds);
        if (mask & AE_WRITABLE) FD_CLR((SOCKET)fd,&state->wfds);
    }
}

static int aeApiPoll(aeEventLoop *eventLoop, struct timeval *tvp) {
    aeApiState *state = eventLoop->apidata;
    int retval, j, numevents = 0;
    DWORD pipe_is_on=0;
	
    memcpy(&state->_rfds,&state->rfds,sizeof(fd_set));
    memcpy(&state->_wfds,&state->wfds,sizeof(fd_set));
	
    retval = select(eventLoop->maxfd+1,
					&state->_rfds,&state->_wfds,NULL,tvp);
	
    if (state->vm_pipe != INVALID_HANDLE_VALUE) {
        if (PeekNamedPipe(state->vm_pipe, NULL, 0, NULL, &pipe_is_on, NULL)) {
            if (pipe_is_on) retval++;
		}
    }
	
    if (retval > 0) {
        for (j = 0; j <= eventLoop->maxfd; j++) {
            int mask = 0;
            aeFileEvent *fe = &eventLoop->events[j];
			
            if (fe->mask == AE_NONE) continue;
            if (fe->mask & AE_READABLE && FD_ISSET(j,&state->_rfds))
                mask |= AE_READABLE;
            if (fe->mask & AE_WRITABLE && FD_ISSET(j,&state->_wfds))
                mask |= AE_WRITABLE;
            if (fe->mask & AE_PIPE && pipe_is_on)
                mask |= AE_READABLE;
            eventLoop->fired[numevents].fd = j;
            eventLoop->fired[numevents].mask = mask;
            numevents++;
        }
    }
    return numevents;
}

static char *aeApiName(void) {
    return "winsock2";
}
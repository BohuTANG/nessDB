#include <stdlib.h>
#include <stdio.h>
#include <libgen.h>
#include <signal.h>
#include <string.h>

#include <err.h>
#include <assert.h>
#include <dlfcn.h>

#include <zmq.h>

#include "db-zmq.h"

struct dbz_s {
	int running;
	void* mod;
	void* mod_ctx;
	void* zctx;
	struct module_feature* features;
};
typedef struct dbz_s dbz;

dbz*
dbz_load(char *filename) {
	dbz* x = malloc(sizeof(dbz));
	memset(x, 0, sizeof(dbz));
	x->mod = dlopen(filename, RTLD_LAZY);
	if( ! x->mod ) {
		warnx("Cannot dlopen('%s') = %p, %s", filename, x->mod, dlerror());
		return NULL;
	}
    void* f = dlsym(x->mod, "i_speak_db");
    if( ! f ) {
    	warnx("Cannot dlsym('i_speak_db') = %p, %s", f, dlerror());
    	dlclose(x->mod);
    	return NULL;
    }

    x->features = ((mod_init_fn) f)();
    x->zctx = zmq_init(1);
    return x;
}

struct module_feature*
dbz_bind(dbz* ctx, const char *name, const char *addr) {
	struct module_feature* f = ctx->features;
	while( f->name ) {
		if( strcmp(f->name, name) == 0 ) {
			break;
		}
		f++;
	}

	if( ! f->name ) {
		warnx("Unknown bind name %s=%s", name, addr);
		return NULL;
	}

	void *sock = zmq_socket(ctx->zctx, f->opts ? ZMQ_REP : ZMQ_PULL);
	if( sock == NULL ) {
		warnx("Cannot create socket for '%s': %s", addr, zmq_strerror(zmq_errno()));	
		return NULL;;
	} 
	if( zmq_bind(sock, addr) == -1 ) {
		warnx("Cannot bind socket '%s': %s", addr, zmq_strerror(zmq_errno()));
		zmq_close(sock);
		return NULL;
	}
	f->token = sock;
	return f;
}

int
dbz_close(dbz* ctx) {
	// TODO: close sockets
	if( ctx->zctx ) zmq_term(ctx->zctx);
	if( ctx->mod ) dlclose(ctx->mod);
	memset(ctx, 0, sizeof(dbz));
	free(ctx);
	return 1;
}

size_t
reply_cb(void* data, size_t len, void* cb, void* token ) {
	assert( cb == NULL );
	zmq_msg_t msg;
	zmq_msg_init_data(&msg, data, len, NULL, NULL);
	zmq_send(token, &msg, 0);
	return len;
}

int
dbz_run(dbz* ctx) {
	ctx->running = 1;
	int fc = 0;
	struct module_feature* f = ctx->features;
	while( (f++)->name ) {
		fc++;
	}
	zmq_pollitem_t items[fc];

	while( ctx->running == 1 ) {
		memset(&items[0], 0, sizeof(zmq_pollitem_t) * fc);
		for( int i = 0; i < fc; i++ ) {
			items[i].socket = ctx->features[i].token;
			items[i].events = ZMQ_POLLIN;
		}
	
		int rc = zmq_poll(items, fc, -1);
		assert( rc >= 0 );
	
		for( int i = 0; i < fc; i++ ) {
			zmq_msg_t msg;
			if( 0 == zmq_recv(f->token, &msg, 0) ) {
				f->cb(zmq_msg_data(&msg), zmq_msg_size(&msg), reply_cb, f->token);
			}
		}
	}
	return ctx->running;
}

int main(int argc, char **argv) {
	if( argc < 2 ) {
		fprintf(stderr, "Usage: %s <module.so> [op=tcp://... ]\n", argv[0]);
		return( EXIT_FAILURE );
	}
	dbz* d = dbz_load(argv[1]);
	if( ! d ) return( EXIT_FAILURE );

	int ok = 0;
	for( int i = 2; i < argc; i++ ) {
		char *op = argv[i];
		char *addr = strchr(op, '=');
		*addr++ = 0;
		struct module_feature* f = dbz_bind(d, op, addr);
		if( ! f  ) {
			errx(EXIT_FAILURE, "Cannot bind '%s'='%s'", op, addr);
		}
		ok += f!=0;
	}

	if( ! ok ) {
		struct module_feature* f = d->features;
		fprintf(stderr, "Operations:\n");
		while( f->name ) {
			fprintf(stderr, "\t%s\n", f->name);
			f++;
		}		
		exit(EXIT_FAILURE);
	}
	dbz_run(d);

	dbz_close(d);
	return( EXIT_SUCCESS );
}

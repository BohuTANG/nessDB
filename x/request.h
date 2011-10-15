#ifndef _REQUEST_H_
#define _REQUEST_H_

struct request{
	char *data;
	char *cmd;
	char *k;
	char *v;
	size_t k_len;
	size_t v_len;
	size_t pos;
};

struct request *request_new();
void request_parse(struct request *request);
void request_free(struct request *request);
void request_dump(struct request *request);

#endif

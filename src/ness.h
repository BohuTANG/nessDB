#ifndef _NESS_H_
#define _NESS_H_

struct nobj{
	char *k;
	void *v;
	int refcount;//reference counts of this object
	struct nobj *next;
};

#define FOR_EACH(NOBJ,LIST) \
	struct nobj *NOBJ;\
	for((NOBJ)=LIST;(NOBJ);(NOBJ)=(NOBJ)->next)\

#endif

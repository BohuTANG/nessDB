#ifndef _DB_H
#define _DB_H
struct log{
	int	fd;
	int	used;
	int	flag;
	char 	magic;
};

struct log_node{
	int 	k_len;
	int	v_len;
	uint64_t v_offset;

	char key[20];
};


void 	db_init(int bufferpool_size);

void*	db_get(char* key);
int 	db_add(char* key,char* value);
void	db_remove(char* key);
void 	db_destroy();

#endif

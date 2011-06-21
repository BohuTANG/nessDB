#include <stdio.h>

typedef struct db
{
	FILE* db_write_ptr;
	FILE* db_read_ptr;
}db_t;


void  db_init(const char* dbname);
size_t   db_write(char* key,size_t k_len,char* value,size_t v_len,size_t offset);
int   db_bulk_write(char* block);
char* db_read(char* key);
void  db_close();

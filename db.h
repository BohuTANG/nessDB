#include <stdio.h>

typedef struct db
{
	FILE* db_write_ptr;
	FILE* db_read_ptr;
}db_t;


void  db_init(const char* dbname);
int   db_write(char* key,char* value);
int   db_bulk_write(char* block);
char* db_read(char* key);
void  db_close();

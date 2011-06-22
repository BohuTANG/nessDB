#include <stdio.h>

typedef struct db
{
	FILE* db_write_ptr;
	FILE* db_read_ptr;
}db_t;


void  	db_init(const char* dbname);
size_t 	db_write(char* key,unsigned int k_len,char* value,unsigned int v_len,size_t offset);
void   	db_bulk_write(char* block,unsigned int b_len);
char* 	db_read(unsigned int index,unsigned int offset);
void  	db_close();

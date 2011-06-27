#include <stdio.h>
#define KLEN__ (1024)
#define VLEN__ (1024*10)

void	db_init(const char* dbname);
void 	db_write(char* key, int k_len,char* value, int v_len);
void   	db_bulk_write(char* block, int b_len);
void 	db_read(int index, char* value);
void  	db_close();

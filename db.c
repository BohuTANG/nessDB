#include "db.h"
#include <stdlib.h>


static db_t* _db;
static size_t _idx=0;

void db_init(const char* dbname)
{
	FILE* fread,*fwrite;
	fread=fopen(dbname,"rb");
	if(fread==NULL)
	{
		fwrite=fopen(dbname,"wb");
		fread=fopen(dbname,"rb");
	}
	else
		fwrite=fopen(dbname,"wb");

	_db=(db_t*)malloc(sizeof(db_t));
	if(fread!=NULL
		&&fwrite!=NULL	
		&&_db!=NULL)
	{
		_db->db_read_ptr=fread;
		_db->db_write_ptr=fwrite;
	}
	else
		printf("db_init error!\n");
}


size_t  db_write(char* key,size_t k_len,char* value,size_t v_len,size_t offset)
{
	fwrite(&k_len,k_len,sizeof(int),_db->db_write_ptr);
	fwrite(&v_len,v_len,sizeof(int),_db->db_write_ptr);
	fwrite(key,k_len,1,_db->db_write_ptr);
	fwrite(value,v_len,1,_db->db_write_ptr);
	size_t tmp=_idx;
	_idx+=offset;
	return tmp;
}

int db_bulk_write(char* block)
{
	
	return (1);
}

char* db_read(char* key)
{
	return NULL;
}

void db_close()
{
	fclose(_db->db_read_ptr);
	fclose(_db->db_write_ptr);
}

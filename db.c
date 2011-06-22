#include <stdlib.h>
#include "db.h"

static db_t* _db;
static unsigned int _idx=0;

void 
db_init(const char* dbname)
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


unsigned int
db_write(char* key,unsigned int k_len,char* value,unsigned int v_len,unsigned int offset)
{
	fwrite(&k_len,k_len,sizeof(int),_db->db_write_ptr);
	fwrite(&v_len,v_len,sizeof(int),_db->db_write_ptr);
	fwrite(key,k_len,1,_db->db_write_ptr);
	fwrite(value,v_len,1,_db->db_write_ptr);
	unsigned int tmp=_idx;
	_idx+=offset;
	return tmp;
}

void 
db_bulk_write(char* block,unsigned int b_len)
{
	fwrite(block,b_len,1,_db->db_write_ptr);
	_idx+=b_len;
}

char* 
db_read(unsigned int index,unsigned int offset)
{
	return NULL;
}

void db_close()
{
	fclose(_db->db_read_ptr);
	fclose(_db->db_write_ptr);
}

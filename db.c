#include "db.h"
#include <stdlib.h>


static db_t* _db;

void db_init(const char* dbname)
{
	FILE* fread=fopen(dbname,"rb");
	if(fread!=NULL)
	{
		FILE* fwrite=fopen(dbname,"wb");
		if(fwrite!=NULL)
		{
			_db=(db_t*)malloc(sizeof(db_t));
			if(_db!=NULL)
			{
				_db->db_read_ptr=fread;
				_db->db_write_ptr=fwrite;
			}
		}
		else
			printf("init write error!\n");
	}
	else
		printf("init read error!\n");
}


int db_write(char* key,char* value)
{
	int k_len=strlen(key);
	int v_len=strlen(value);
	fwrite(&k_len,k_len,sizeof(int),_db->db_write_ptr);
	fwrite(&v_len,v_len,sizeof(int),_db->db_write_ptr);
	fwrite(key,k_len,1,_db->db_write_ptr);
	fwrite(value,v_len,1,_db->db_write_ptr);
		
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

/*	db operate layer
 *	overred@gmail.com
 */
#include <stdlib.h>
#include <string.h>
#include "debug.h"
#include "db.h"

typedef struct db
{
	FILE* db_write_ptr;
	FILE* db_read_ptr;
}db_t;

static db_t* _db;

void 
db_init(const char* dbname)
{
	FILE* fread=NULL,*fwrite=NULL,*fload=NULL;
	fread=fopen(dbname,"rb");
	if(fread==NULL)
	{
		fwrite=fopen(dbname,"wb");
		fread=fopen(dbname,"rb");
	}
	else
		fwrite=fopen(dbname,"ab");

	_db=(db_t*)malloc(sizeof(db_t));
	if(fread!=NULL
		&&fwrite!=NULL	
		&&_db!=NULL)
	{
		_db->db_read_ptr=fread;
		_db->db_write_ptr=fwrite;
	}
	else
		INFO("db_init error!");
}


void
db_write(char* key, int k_len,char* value, int v_len)
{
	fwrite(&k_len,1,sizeof(int),_db->db_write_ptr);
	fwrite(&v_len,1,sizeof(int),_db->db_write_ptr);
	fwrite(key,k_len,1,_db->db_write_ptr);
	fwrite(value,v_len,1,_db->db_write_ptr);
	fflush(_db->db_write_ptr);
}

void 
db_bulk_write(char* block, int b_len)
{
	fwrite(block,b_len,1,_db->db_write_ptr);
	fflush(_db->db_write_ptr);
}

void
db_read(int index, int offset,char* value)
{
	int k_len=0;
	int v_len=0;
	char v_tmp[VLEN__]={0};

	fseek(_db->db_read_ptr,index,SEEK_SET);
	fread(&k_len,sizeof(int),1,_db->db_read_ptr);
	fread(&v_len,sizeof(int),1,_db->db_read_ptr);
	//skip k_len
	fseek(_db->db_read_ptr,k_len,SEEK_CUR);
	fread(&v_tmp,v_len,1,_db->db_read_ptr);
	memcpy(value,v_tmp,v_len);
}

void db_close()
{
	fclose(_db->db_read_ptr);
	fclose(_db->db_write_ptr);
}

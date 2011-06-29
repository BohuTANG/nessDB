/*	db operate layer
 *	overred@gmail.com
 */
#include <stdlib.h>
#include <string.h>
#include "debug.h"
#include "db.h"

typedef struct db
{
	FILE*	db_write_ptr;
	FILE*	db_read_ptr;
	FILE*	db_write_idx_ptr;
}db_t;

static db_t* _db;

void 
db_init(const char* dbname,const char* idxname)
{
	FILE* fread=NULL,*fwrite=NULL,*fwrite_idx=NULL;
	fread=fopen(dbname,"rb");
	if(fread==NULL)
	{
		fwrite=fopen(dbname,"wb");
		fread=fopen(dbname,"rb");

		fwrite_idx=fopen(idxname,"wb");
	}
	else
	{
		fwrite=fopen(dbname,"ab");
		fwrite_idx=fopen(idxname,"ab");
	}

	_db=(db_t*)malloc(sizeof(db_t));
	if(fread!=NULL
		&&fwrite!=NULL
		&&fwrite_idx!=NULL
		&&_db!=NULL)
	{
		_db->db_read_ptr=fread;
		_db->db_write_ptr=fwrite;

		_db->db_write_idx_ptr=fwrite_idx;
	}
	else
		INFO("db_init error!");
}


void
db_write(char* key, int k_len,char* value, int v_len)
{
	fwrite(&k_len,1,sizeof(int),_db->db_write_idx_ptr);
	fwrite(&v_len,1,sizeof(int),_db->db_write_idx_ptr);
	fwrite(key,k_len,1,_db->db_write_idx_ptr);

	fwrite(value,v_len,1,_db->db_write_ptr);

//	fflush(_db->db_write_idx_ptr);
//	fflush(_db->db_write_ptr);
}



void 
db_bulk_write(char* block, int b_len)
{
	fwrite(block,b_len,1,_db->db_write_ptr);
//	fflush(_db->db_write_ptr);
}


void 
db_bulk_write_idx(char* idx_block, int b_len)
{
	fwrite(idx_block,b_len,1,_db->db_write_idx_ptr);
//	fflush(_db->db_write_idx_ptr);
}


void
db_read(int index,int offset, char* value)
{
	char v_tmp[VLEN__]={0};

	fseek(_db->db_read_ptr,index,SEEK_SET);
	fread(&v_tmp,offset,1,_db->db_read_ptr);
	memcpy(value,v_tmp,offset);
}

void db_close()
{
	fclose(_db->db_read_ptr);
	fclose(_db->db_write_ptr);

	fclose(_db->db_write_idx_ptr);
}

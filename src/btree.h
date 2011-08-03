#define BLOCK_NUM 10
#define KEY_SIZE 16

typedef struct block
{
	char key[KEY_SIZE];	
	int val_offset;
	int val_length;
}block_t;

typedef struct bucket
{
	int used;
//	int level;//level_1=level_0*10
	block_t blocks[BLOCK_NUM];
	int nxt_bucket_offset;
}bucket_t;

typedef struct btree
{
	//w* write only ;r* read only
	int idx_wfd0;
	int idx_rfd0;

/*
	int idx_wfd1;
	int idx_rfd1;
*/

	int db_wfd;
	int db_rfd;

	int index_offset;
	int db_offset;
	int cap;

}btree_t;


void 	btree_init(int capacity);
void 	btree_add(char *key,char *value);
int		btree_get(char *key,char *value);

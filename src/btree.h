
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
	int level;//level_1=level_0*10
	block_t *blocks;
	int nxt_bucket_offset;
}bucket_t;

typedef struct btree
{
	int fd0;
	int fd1;

	int index_offset;
	int db_offset;
	int cap;

}btree_t;


void 	btree_init(int capacity);
void 	btree_add(char *key,char *value);
int		btree_get(char *key,char *value);

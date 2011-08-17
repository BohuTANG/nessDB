#define BLOCK_SIZE (sizeof(block_t))
#define BLOCK_NUM ((1024*4-1)/BLOCK_SIZE)
#define KEY_SIZE 16

typedef struct block
{
	char key[KEY_SIZE];	
	int child;

	int val_offset;
}block_t;

typedef struct page
{
	int used;
	block_t blocks[BLOCK_NUM];
}page_t;

typedef struct blob
{
	size_t	len;
	char	*val;
}blob_t;


void 	btree_init(int capacity);
void 	btree_add(char *key,char *value);
int		btree_get(char *key,char *value);
void	btree_destroy();

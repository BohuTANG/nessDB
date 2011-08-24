#include <stdint.h>
#define KEYSIZE 64

typedef struct hashtable hashtable;
typedef struct hashtable_entry hashtable_entry;

hashtable *hashtable_create(int capacity);
void hashtable_set(hashtable *t,char *key,uint64_t value);
uint64_t hashtable_get(hashtable *t,char *key);
void hashtable_remove(hashtable *t,char *key);


void hashtable_destroy(hashtable *t);
hashtable_entry *hashtable_body_allocate(unsigned int capacity);

unsigned int hashtable_find_slot(hashtable *t,char *key);
unsigned long hashtable_hash(char *str);
int hashtable_count(hashtable* t);

struct hashtable 
{
	unsigned int size;
	unsigned int capacity;
	hashtable_entry* body;
};

struct hashtable_entry 
{
	char key[KEYSIZE];
	int touch;
	uint64_t value;
};

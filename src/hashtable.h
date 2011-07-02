typedef struct hashtable hashtable;
void hashtable_destroy(hashtable *t);
typedef struct hashtable_entry hashtable_entry;
hashtable_entry *hashtable_body_allocate(unsigned int capacity);
hashtable *hashtable_create(int cap);
void hashtable_remove(hashtable *t,char *key);
void hashtable_resize(hashtable *t,unsigned int capacity);
void hashtable_set(hashtable *t,char *key,void *value);
void *hashtable_get(hashtable *t,char *key);
unsigned int hashtable_find_slot(hashtable *t,char *key);
unsigned long hashtable_hash(char *str);
int hashtable_count(hashtable* t);
struct hashtable {
	unsigned int size;
	unsigned int capacity;
	hashtable_entry* body;
};
struct hashtable_entry {
	char* key;
	void* value;
	struct hashtable_entry* next;

};

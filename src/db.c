#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sys/stat.h>

#include "btree.h"
#include "hashtable.h"
#include "db.h"

#define DBNAME "ness.ndb"
#define MAX_HITS (1024)

typedef struct pointer
{
	int	val_offset;
}pointer_t;

static hashtable*	_ht;
struct btree 		_btree;


static int file_exists(const char *path)
{
        struct stat st;
        return stat(path, &st) == 0;
}


void db_init(int capacity)
{
	_ht=hashtable_create(capacity);
	if (file_exists(DBNAME))
	{
		if (btree_open(&_btree,DBNAME)) 
			printf("Unable to open database\n");
	} 
	else 
	{
		if (btree_creat(&_btree,DBNAME))
			printf("Unable to create database\n");
	}
}

int db_add(char* key,char* value)
{
	void* entry=hashtable_get(_ht,key);
	if(entry==NULL)
	{	
		size_t val_offset=btree_insert(&_btree,key,value,strlen(value));
		//add table
		hashtable_set(_ht,key,(void*)val_offset);
	}
	return (1);
}


void
db_load_index()
{
	//todo
}

void *db_get(char* key)
{
	char* lru_v=NULL;
	void* entry=hashtable_get(_ht,key);
	if(entry!=NULL)
		return btree_get_value(&_btree,(size_t)entry);
	else
		return btree_get(&_btree,key);
}

void
db_remove(char* key)
{
	//todo
}

void db_destroy()
{
	btree_close(&_btree);
}

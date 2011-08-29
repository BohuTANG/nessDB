/**
 * Compact-Hash-Tree implementation,used for LRU
 */

#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <string.h>
#include "hashtable.h"
#include "hashes.h"

//if over this num,entry will be long alive on index slot.
#define G 16


static unsigned int _primes[28]={53, 97, 193, 389, 769, 1543, 3079, 6151, 12289,
				24593, 49157, 98317, 196613, 393241, 786433, 1572869,
				3145739, 6291469, 12582917, 25165843, 50331653,
				100663319, 201326611, 402653189, 805306457, 1610612741};	

static unsigned int hashtable_get_prime(int maxnum)
{
	return (maxnum+23);
}


unsigned int hashtable_find_slot(hashtable* t, char* key)
{
	return  t->hashfunc(key) % t->capacity;
}


uint64_t hashtable_get(hashtable* t, char* key)
{
	int index = hashtable_find_slot(t, key);
	if (t->body[index].value !=0 && strcmp(t->body[index].key,key)==0) 
		return t->body[index].value;

	return 0;
}


void hashtable_set(hashtable* t, char* key, uint64_t value)
{
	int index = hashtable_find_slot(t, key);
	if (t->body[index].value !=0) 
	{
		if(strcmp(t->body[index].key,key)==0)
			t->body[index].touch++;
		else
		{
			if(t->body[index].touch>G)
				return;
			else
			{
				strcpy(t->body[index].key,key);		
				t->body[index].value=value;
			}
				
		}
	} 
	else 
	{
		t->size++;
		strcpy(t->body[index].key,key);	
		t->body[index].value=value;
	}
}

void hashtable_remove(hashtable *t,char *key)
{
	int index = hashtable_find_slot(t, key);
	if (t->body[index].value !=0) 
	{
		memset(&t->body[index].key,sizeof(char),KEYSIZE);
		t->body[index].touch=0;
		t->body[index].value=0;
		t->size--;	
	}
}

hashtable* hashtable_create(int cap)
{
	hashtable* new_ht = malloc(sizeof(hashtable));
	new_ht->size = 0;
	new_ht->capacity = hashtable_get_prime(cap);
	new_ht->body = hashtable_body_allocate(new_ht->capacity);
	new_ht->hashfunc=jdb_hash;
	new_ht->hashfunc1=sdbm_hash;
	return new_ht;
}


hashtable_entry* hashtable_body_allocate(unsigned int capacity)
{
	return (hashtable_entry*)calloc(capacity, sizeof(hashtable_entry));
}



void hashtable_destroy(hashtable* t)
{
	free(t->body);
	free(t);
}

int hashtable_count(hashtable *t)
{
	return t->size;	
}


/**
 *	Compact-Hash-Tree implementation
 * (c) overred based on @marekweb,thx
 *
 * Uses dynamic addressing with linear probing.
 */

#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <string.h>
#include "hashtable.h"
#include "crc16.h"

static unsigned long _primes[]={53UL, 97UL, 193UL, 389UL, 769UL, 1543UL, 3079UL, 6151UL, 12289UL,
			24593UL, 49157UL, 98317UL, 196613UL, 393241UL, 786433UL, 1572869UL,
			3145739UL, 6291469UL, 12582917UL, 25165843UL, 50331653UL,
			100663319UL, 201326611UL, 402653189UL, 805306457UL, 1610612741UL,
			3221225473UL, 4294967291UL};	

/**
 * Compute the hash value for the given string.
 * Implements the djb k=33 hash function.
 */
unsigned long hashtable_hash(char* str)
{
	unsigned long hash = 5381;
	int c;
	while ((c = *str++))
		hash = ((hash << 5) + hash) + c;  /* hash * 33 + c */
	return hash;
}

/**
 *compute the crc value for the hash collision
*/

static  unsigned int hashtable_find_crc(char* str)
{
	unsigned int crc= crc16(str,strlen(str));	
	if(crc==0)
		crc=1;
	return crc;
}

static long hashtable_primer(long k)
{
	int size=sizeof(_primes)/sizeof(*_primes);
	int i;
	for(i=(size-1);i>0;i--)
	{
		long p=_primes[i];
		if(k>=p)
			break;

	}
	return _primes[++i];
}

/**
 * Find an available slot for the given key, using linear probing.
 */
unsigned int hashtable_find_slot(hashtable* t, char* key)
{
	return  hashtable_hash(key) % t->capacity;
}

/**
 * Return the item associated with the given key, or NULL if not found.
 */
void* hashtable_get(hashtable* t, char* key)
{
	int index = hashtable_find_slot(t, key);
	unsigned int crc=hashtable_find_crc(key);
	if (t->body[index].crc !=0) {
		hashtable_entry* cur=&t->body[index];
		while(cur!=NULL)
		{
			if(cur->crc==crc)
				return cur->value;
			cur=cur->next;
		}

		printf("crc %d,key:%s \n",crc,key);
		return NULL;
	}
}

/**
 * Assign a value to the given key in the table.
 */
void hashtable_set(hashtable* t, char* key, void* value)
{
	int index = hashtable_find_slot(t, key);
	unsigned int crc=hashtable_find_crc(key);
	if (t->body[index].crc != 0) {
		hashtable_entry* etmp=hashtable_body_allocate(1);
		etmp->crc= crc;
		etmp->value = value;

		hashtable_entry*  cur=&t->body[index];
		if(cur->next==NULL)
		{
			cur->next=etmp;
			return;
		}

		while(cur->next!=NULL)
			cur=cur->next;
	
		cur->next=etmp;

	} else {
		t->size++;
		t->body[index].crc=crc;		
		t->body[index].value=value;

	}
}

/**
 * Remove a key from the table
 */
void hashtable_remove(hashtable* t, char* key)
{
	int index = hashtable_find_slot(t, key);
	unsigned int crc=hashtable_find_crc(key);
	if(t->body[index].crc!=0)
	{
		hashtable_entry* cur=&t->body[index];
		hashtable_entry* pre=cur;	
		while(cur!=NULL)
		{
			if(cur->crc==crc)
			{
					t->size--;
					pre->next=cur->next;
					if(cur==&t->body[index])
						t->body[index]=*cur->next;
					free(cur);
					return;
			}
			pre=cur;
			cur=cur->next;
		}
	}
}

/**
 * Create a new, empty hashtable
 */
hashtable* hashtable_create(int cap)
{
	hashtable* new_ht = malloc(sizeof(hashtable));
	new_ht->size = 0;
	new_ht->capacity = cap;//HASHTABLE_INITIAL_CAPACITY;
	new_ht->body = hashtable_body_allocate(new_ht->capacity);
	return new_ht;
}

/**
 * Allocate a new memory block with the given capacity.
 */
hashtable_entry* hashtable_body_allocate(unsigned int capacity)
{
	return (hashtable_entry*)calloc(capacity, sizeof(hashtable_entry));
}


/**
 * Destroy the table and deallocate it from memory. This does not deallocate the contained items.
 */
void hashtable_destroy(hashtable* t)
{
	free(t->body);
	free(t);
}

int hashtable_count(hashtable *t)
{
	return t->size;	
}


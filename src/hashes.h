#ifndef _HASHES_H
#define _HASHES_H
#include <string.h>
/**
 * SAX hash function
 */
static inline unsigned int sax_hash(const char *key)
{
    unsigned int h = 0;
    while (*key) {
        h ^= (h << 5) + (h >> 2) + (unsigned char) *key;
        ++key;
    }
    return h;
}

/**
 * SDBM hash function
 */
static inline unsigned int sdbm_hash(const char *key)
{
    unsigned int h = 0;
    while (*key) {
        h = (unsigned char) *key + (h << 6) + (h << 16) - h;
        ++key;
    }
    return h;
}

/**
 * Murmur2 hash function
 */
static inline unsigned murmur_hash(const char *key)
{
    if (!key) {
        return 0;
    }
    unsigned m = 0x5bd1e995;
    unsigned r = 24;
    unsigned seed = 0xdeadbeef;
    size_t len = strlen(key);
    unsigned h = seed ^ len;
    while (len >= 4) {
        unsigned k = *(unsigned*)key;
        k *= m;
        k ^= k >> r;
        k *= m;
        h *= m;
        h ^= k;
        key += 4;
        len -= 4;
    }
    switch(len) {
    case 3:
        h ^= key[2] << 16;
    case 2:
        h ^= key[1] << 8;
    case 1:
        h ^= key[0];
        h *= m;
    };
    h ^= h >> 13;
    h *= m;
    h ^= h >> 15;
    return h;
}

/**
 * Jenkins hash function
 */
static inline unsigned jenkins_hash(const char *key)
{
    if (!key) {
        return 0;
    }
    unsigned hash, i;
    size_t len = strlen(key);
    for (hash = i = 0; i < len; ++i) {
        hash += key[i];
        hash += (hash << 10);
        hash ^= (hash >> 6);
    }
    hash += (hash << 3);
    hash ^= (hash >> 11);
    hash += (hash << 15);
    return hash;
}

/**
 * Djb hash function
 */
static inline size_t djb_hash(const char* key)
{
    if (!key) {
        return 0;
    }
    unsigned int hash = 5381;
    unsigned int c;
    while ((c = *key++))
	hash = ((hash << 5) + hash) + (unsigned int)c;  /* hash * 33 + c */

   return (size_t) hash;
}

#endif

#ifndef _BITWISE_H
#define _BITWISE_H

#include <stdint.h>
#include <fcntl.h>

#ifdef _WIN32
	#include <winsock.h>
#else
/* Unix */
#include <sys/types.h>
#include <netinet/in.h>
#include <arpa/inet.h> /* htonl/ntohl */

#endif

//bloom
#define CHAR_BIT 8
#define SETBIT_1(bitset,i) (bitset[i / CHAR_BIT] |=  (1<<(i % CHAR_BIT)))
#define SETBIT_0(bitset,i) (bitset[i / CHAR_BIT] &=  (~(1<<(i % CHAR_BIT))))
#define GETBIT(bitset,i) (bitset[i / CHAR_BIT] &   (1<<(i % CHAR_BIT)))


//byte order
#ifdef __CHECKER__
#define FORCE           __attribute__((force))
#else
#define FORCE
#endif

#ifdef __CHECKER__
#define BITWISE         __attribute__((bitwise))
#else
#define BITWISE
#endif

typedef uint16_t BITWISE __be16; /* big endian, 16 bits */
typedef uint32_t BITWISE __be32; /* big endian, 32 bits */
typedef uint64_t BITWISE __be64; /* big endian, 64 bits */

static inline __be32 to_be32(uint32_t x)
{
	return (FORCE __be32) htonl(x);
}

static inline __be16 to_be16(uint16_t x)
{
	return (FORCE __be16) htons(x);
}

static inline __be64 to_be64(uint64_t x)
{
#if (BYTE_ORDER == LITTLE_ENDIAN)
	return (FORCE __be64) (((uint64_t) htonl((uint32_t) x) << 32) |
			       htonl((uint32_t) (x >> 32)));
#else
	return (FORCE __be64) x;
#endif
}

static inline uint32_t from_be32(__be32 x)
{
	return ntohl((FORCE uint32_t) x);
}

static inline uint16_t from_be16(__be16 x)
{
	return ntohs((FORCE uint16_t) x);
}

static inline uint64_t from_be64(__be64 x)
{
#if (BYTE_ORDER == LITTLE_ENDIAN)
	return ((uint64_t) ntohl((uint32_t) (FORCE uint64_t) x) << 32) |
		ntohl((uint32_t) ((FORCE uint64_t) x >> 32));
#else
	return (FORCE uint64_t) x;
#endif
}


//get H bit
static inline int get64_H(uint64_t x)
{
	if(((x>>63)&0x01)!=0x01)
		return 0;
	else
		return 1;
}

static inline int get32_H(uint32_t x)
{
	if(((x>>31)&0x01)!=0x01)
		return 0;
	else
		return 1;
}

//set H bit to 0
static inline uint64_t set64_H_0(uint64_t x)
{
	return	x&=0x3FFFFFFFFFFFFFFF;
}

//set H bit to 0
static inline uint32_t set32_H_0(uint32_t x)
{
	return	x&=0x3FFFFFFF;
}

//set H bit to 1
static inline uint64_t set64_H_1(uint64_t x)
{
	return  x|=0x8000000000000000;	
}

static inline uint32_t set32_H_1(uint32_t x)
{
	return  x|=0x80000000;	
}

/* Return a value that is greater or equal to 'val' and is power-of-two. */
static inline size_t round_power2(size_t val)
{
	size_t i = 1;
	while (i < val)
		i <<= 1;
	return i;
}

#endif

/* http://oauth.googlecode.com/svn/code/c/liboauth/src/sha1.c
 * Notes:
 * This code is public-domain - it is based on libcrypt 
 * placed in the public domain by Wei Dai and other contributors.
 */
// gcc -Wall -DSHA1TEST -o sha1test sha1.c && ./sha1test

#include <stdint.h>
#include <string.h>
#include "sha1.h"

/* code */
#define SHA1_K0 0x5a827999
#define SHA1_K20 0x6ed9eba1
#define SHA1_K40 0x8f1bbcdc
#define SHA1_K60 0xca62c1d6

const uint8_t sha1InitState[] = {
  0x01,0x23,0x45,0x67, // H0
  0x89,0xab,0xcd,0xef, // H1
  0xfe,0xdc,0xba,0x98, // H2
  0x76,0x54,0x32,0x10, // H3
  0xf0,0xe1,0xd2,0xc3  // H4
};

void sha1_init(sha1nfo *s) {
  memcpy(s->state.b,sha1InitState,HASH_LENGTH);
  s->byteCount = 0;
  s->bufferOffset = 0;
}

uint32_t sha1_rol32(uint32_t number, uint8_t bits) {
  return ((number << bits) | (number >> (32-bits)));
}

void sha1_hashBlock(sha1nfo *s) {
  uint8_t i;
  uint32_t a,b,c,d,e,t;

  a=s->state.w[0];
  b=s->state.w[1];
  c=s->state.w[2];
  d=s->state.w[3];
  e=s->state.w[4];
  for (i=0; i<80; i++) {
    if (i>=16) {
      t = s->buffer.w[(i+13)&15] ^ s->buffer.w[(i+8)&15] ^ s->buffer.w[(i+2)&15] ^ s->buffer.w[i&15];
      s->buffer.w[i&15] = sha1_rol32(t,1);
    }
    if (i<20) {
      t = (d ^ (b & (c ^ d))) + SHA1_K0;
    } else if (i<40) {
      t = (b ^ c ^ d) + SHA1_K20;
    } else if (i<60) {
      t = ((b & c) | (d & (b | c))) + SHA1_K40;
    } else {
      t = (b ^ c ^ d) + SHA1_K60;
    }
    t+=sha1_rol32(a,5) + e + s->buffer.w[i&15];
    e=d;
    d=c;
    c=sha1_rol32(b,30);
    b=a;
    a=t;
  }
  s->state.w[0] += a;
  s->state.w[1] += b;
  s->state.w[2] += c;
  s->state.w[3] += d;
  s->state.w[4] += e;
}

void sha1_addUncounted(sha1nfo *s, uint8_t data) {
  s->buffer.b[s->bufferOffset ^ 3] = data;
  s->bufferOffset++;
  if (s->bufferOffset == BLOCK_LENGTH) {
    sha1_hashBlock(s);
    s->bufferOffset = 0;
  }
}

void sha1_writebyte(sha1nfo *s, uint8_t data) {
  ++s->byteCount;
  sha1_addUncounted(s, data);
}

void sha1_write(sha1nfo *s, const char *data, size_t len) {
	for (;len--;) sha1_writebyte(s, (uint8_t) *data++);
}

void sha1_pad(sha1nfo *s) {
  // Implement SHA-1 padding (fips180-2 Â§5.1.1)

  // Pad with 0x80 followed by 0x00 until the end of the block
  sha1_addUncounted(s, 0x80);
  while (s->bufferOffset != 56) sha1_addUncounted(s, 0x00);

  // Append length in the last 8 bytes
  sha1_addUncounted(s, 0); // We're only using 32 bit lengths
  sha1_addUncounted(s, 0); // But SHA-1 supports 64 bit lengths
  sha1_addUncounted(s, 0); // So zero pad the top bits
  sha1_addUncounted(s, s->byteCount >> 29); // Shifting to multiply by 8
  sha1_addUncounted(s, s->byteCount >> 21); // as SHA-1 supports bitstreams as well as
  sha1_addUncounted(s, s->byteCount >> 13); // byte.
  sha1_addUncounted(s, s->byteCount >> 5);
  sha1_addUncounted(s, s->byteCount << 3);
}

uint8_t* sha1_result(sha1nfo *s) {
  int i;
  // Pad to complete the last block
  sha1_pad(s);
  
  // Swap byte order back
  for (i=0; i<5; i++) {
    uint32_t a,b;
    a=s->state.w[i];
    b=a<<24;
    b|=(a<<8) & 0x00ff0000;
    b|=(a>>8) & 0x0000ff00;
    b|=a>>24;
    s->state.w[i]=b;
  }
  
  // Return pointer to hash (20 characters)
  return s->state.b;
}

#define HMAC_IPAD 0x36
#define HMAC_OPAD 0x5c

void sha1_initHmac(sha1nfo *s, const uint8_t* key, int keyLength) {
  uint8_t i;
  memset(s->keyBuffer, 0, BLOCK_LENGTH);
  if (keyLength > BLOCK_LENGTH) {
    // Hash long keys
    sha1_init(s);
    for (;keyLength--;) sha1_writebyte(s, *key++);
    memcpy(s->keyBuffer, sha1_result(s), HASH_LENGTH);
  } else {
    // Block length keys are used as is
    memcpy(s->keyBuffer, key, keyLength);
  }
  // Start inner hash
  sha1_init(s);
  for (i=0; i<BLOCK_LENGTH; i++) {
    sha1_writebyte(s, s->keyBuffer[i] ^ HMAC_IPAD);
  }
}

uint8_t* sha1_resultHmac(sha1nfo *s) {
  uint8_t i;
  // Complete inner hash
  memcpy(s->innerHash,sha1_result(s),HASH_LENGTH);
  // Calculate outer hash
  sha1_init(s);
  for (i=0; i<BLOCK_LENGTH; i++) sha1_writebyte(s, s->keyBuffer[i] ^ HMAC_OPAD);
  for (i=0; i<HASH_LENGTH; i++) sha1_writebyte(s, s->innerHash[i]);
  return sha1_result(s);
}

/* self-test */

#if SHA1TEST
#include <stdio.h>

uint8_t hmacKey1[]={
   0x00,0x01,0x02,0x03,0x04,0x05,0x06,0x07,0x08,0x09,0x0a,0x0b,0x0c,0x0d,0x0e,0x0f,
   0x10,0x11,0x12,0x13,0x14,0x15,0x16,0x17,0x18,0x19,0x1a,0x1b,0x1c,0x1d,0x1e,0x1f,
   0x20,0x21,0x22,0x23,0x24,0x25,0x26,0x27,0x28,0x29,0x2a,0x2b,0x2c,0x2d,0x2e,0x2f,
   0x30,0x31,0x32,0x33,0x34,0x35,0x36,0x37,0x38,0x39,0x3a,0x3b,0x3c,0x3d,0x3e,0x3f
};
uint8_t hmacKey2[]={
   0x30,0x31,0x32,0x33,0x34,0x35,0x36,0x37,0x38,0x39,0x3a,0x3b,0x3c,0x3d,0x3e,0x3f,
   0x40,0x41,0x42,0x43
};
uint8_t hmacKey3[]={
   0x50,0x51,0x52,0x53,0x54,0x55,0x56,0x57,0x58,0x59,0x5a,0x5b,0x5c,0x5d,0x5e,0x5f,
   0x60,0x61,0x62,0x63,0x64,0x65,0x66,0x67,0x68,0x69,0x6a,0x6b,0x6c,0x6d,0x6e,0x6f,
   0x70,0x71,0x72,0x73,0x74,0x75,0x76,0x77,0x78,0x79,0x7a,0x7b,0x7c,0x7d,0x7e,0x7f,
   0x80,0x81,0x82,0x83,0x84,0x85,0x86,0x87,0x88,0x89,0x8a,0x8b,0x8c,0x8d,0x8e,0x8f,
   0x90,0x91,0x92,0x93,0x94,0x95,0x96,0x97,0x98,0x99,0x9a,0x9b,0x9c,0x9d,0x9e,0x9f,
   0xa0,0xa1,0xa2,0xa3,0xa4,0xa5,0xa6,0xa7,0xa8,0xa9,0xaa,0xab,0xac,0xad,0xae,0xaf,
   0xb0,0xb1,0xb2,0xb3
};
uint8_t hmacKey4[]={
   0x70,0x71,0x72,0x73,0x74,0x75,0x76,0x77,0x78,0x79,0x7a,0x7b,0x7c,0x7d,0x7e,0x7f,
   0x80,0x81,0x82,0x83,0x84,0x85,0x86,0x87,0x88,0x89,0x8a,0x8b,0x8c,0x8d,0x8e,0x8f,
   0x90,0x91,0x92,0x93,0x94,0x95,0x96,0x97,0x98,0x99,0x9a,0x9b,0x9c,0x9d,0x9e,0x9f,
   0xa0
};

void printHash(uint8_t* hash) {
  int i;
  for (i=0; i<20; i++) {
    printf("%02x", hash[i]);
  }
  printf("\n");
}


int main (int argc, char **argv) {
  uint32_t a;
  sha1nfo s;

  // SHA tests
  printf("Test: FIPS 180-2 C.1 and RFC3174 7.3 TEST1\n");
  printf("Expect:a9993e364706816aba3e25717850c26c9cd0d89d\n");
  printf("Result:");
  sha1_init(&s);
  sha1_write(&s, "abc", 3);
  printHash(sha1_result(&s));
  printf("\n\n");

  printf("Test: FIPS 180-2 C.2 and RFC3174 7.3 TEST2\n");
  printf("Expect:84983e441c3bd26ebaae4aa1f95129e5e54670f1\n");
  printf("Result:");
  sha1_init(&s);
  sha1_write(&s, "abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq", 56);
  printHash(sha1_result(&s));
  printf("\n\n");
  
  printf("Test: RFC3174 7.3 TEST4\n");
  printf("Expect:dea356a2cddd90c7a7ecedc5ebb563934f460452\n");
  printf("Result:");
  sha1_init(&s);
  for (a=0; a<80; a++) sha1_write(&s, "01234567", 8);
  printHash(sha1_result(&s));
  printf("\n\n");

  // HMAC tests
  printf("Test: FIPS 198a A.1\n");
  printf("Expect:4f4ca3d5d68ba7cc0a1208c9c61e9c5da0403c0a\n");
  printf("Result:");
  sha1_initHmac(&s, hmacKey1, 64);
  sha1_write(&s, "Sample #1",9);
  printHash(sha1_resultHmac(&s));
  printf("\n\n");

  printf("Test: FIPS 198a A.2\n");
  printf("Expect:0922d3405faa3d194f82a45830737d5cc6c75d24\n");
  printf("Result:");
  sha1_initHmac(&s, hmacKey2, 20);
  sha1_write(&s, "Sample #2", 9);
  printHash(sha1_resultHmac(&s));
  printf("\n\n");

  printf("Test: FIPS 198a A.3\n");
  printf("Expect:bcf41eab8bb2d802f3d05caf7cb092ecf8d1a3aa\n");
  printf("Result:");
  sha1_initHmac(&s, hmacKey3,100);
  sha1_write(&s, "Sample #3", 9);
  printHash(sha1_resultHmac(&s));
  printf("\n\n");

  printf("Test: FIPS 198a A.4\n");
  printf("Expect:9ea886efe268dbecce420c7524df32e0751a2a26\n");
  printf("Result:");
  sha1_initHmac(&s, hmacKey4,49);
  sha1_write(&s, "Sample #4", 9);
  printHash(sha1_resultHmac(&s));
  printf("\n\n");
 
  // Long tests 
  printf("Test: FIPS 180-2 C.3 and RFC3174 7.3 TEST3\n");
  printf("Expect:34aa973cd4c4daa4f61eeb2bdbad27316534016f\n");
  printf("Result:");
  sha1_init(&s);
  for (a=0; a<1000000; a++) sha1_writebyte(&s, 'a');
  printHash(sha1_result(&s));

  return 0;
}
#endif /* self-test */
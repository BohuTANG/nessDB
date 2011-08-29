#include "bloom.h"

#define SETBIT_1(bitset,i) (bitset[i / CHAR_BIT] |=  (1<<(i % CHAR_BIT)))
#define SETBIT_0(bitset,i) (bitset[i / CHAR_BIT] &=  (~(1<<(i % CHAR_BIT))))
#define GETBIT  (bitset,i) (bitset[i / CHAR_BIT] &   (1<<(i % CHAR_BIT)))

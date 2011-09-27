 /* Copyright (c) 2011, BohuTANG <overred.shuttler at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include <stdlib.h>
#include <stdarg.h>
#include "hashes.h"
#include "bitwise.h"
#include "bloom.h"


#define HFUNCNUM 4
void bloom_init(struct bloom *bloom,int size)
{
	bloom->size=size;
	bloom->bitset=calloc((size+1)/CHAR_BIT,sizeof(char));
	bloom->hashfuncs=malloc(HFUNCNUM*sizeof(hashfuncs));

	bloom->hashfuncs[0]=sax_hash;
	bloom->hashfuncs[1]=sdbm_hash;
	bloom->hashfuncs[2]=murmur_hash;
	bloom->hashfuncs[3]=jenkins_hash;
}

void bloom_add(struct bloom *bloom,const char *k)
{
	if(!k)
		return;

	int i;
	for(i=0;i<HFUNCNUM;i++)
	{
		int bit=bloom->hashfuncs[i](k)%bloom->size;
		SETBIT_1(bloom->bitset,bit);
	}
	bloom->count++;
}

int bloom_get(struct bloom *bloom,const char *k)
{
	if(!k)
		return -1;

	int i;
	for(i=0;i<HFUNCNUM;i++)
	{
		int bit=bloom->hashfuncs[i](k)%bloom->size;
		if(GETBIT(bloom->bitset,bit)==0)
			return -1;
	}
	return 0;
}

void bloom_free(struct bloom *bloom)
{
	if(bloom->bitset)
		free(bloom->bitset);
	
	if(bloom->hashfuncs)
		free(bloom->hashfuncs);
}


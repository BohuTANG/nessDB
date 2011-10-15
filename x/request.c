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
#include <stdio.h>
#include <string.h>
#include "request.h"

#define BUF_SIZE (1024*10)

//==============================DFA states=================================//
size_t _table[256]={
/*   0 nul    1 soh    2 stx    3 etx    4 eot    5 enq    6 ack    7 bel  */
        0,       0,       0,       0,       0,       0,       0,       0,
/*   8 bs     9 ht    10 nl    11 vt    12 np    13 cr    14 so    15 si   */
        0,       0,       3,       0,       0,       3,       0,       0,
/*  16 dle   17 dc1   18 dc2   19 dc3   20 dc4   21 nak   22 syn   23 etb */
        0,       0,       0,       0,       0,       0,       0,       0,
/*  24 can   25 em    26 sub   27 esc   28 fs    29 gs    30 rs    31 us  */
        0,       0,       0,       0,       0,       0,       0,       0,
/*  32 sp    33  !    34  "    35  #    36  $    37  %    38  &    39  '  */
        0,       0,       0,       0,       0,       0,       0,       0,
/*  40  (    41  )    42  *    43  +    44  ,    45  -    46  .    47  /  */
        0,       0,       0,       0,       0,       0,       0,       0,
/*  48  0    49  1    50  2    51  3    52  4    53  5    54  6    55  7  */
        2,       2,       2,       2,       2,       2,       2,       2,
/*  56  8    57  9    58  :    59  ;    60  <    61  =    62  >    63  ?  */
        2,       2,       0,       0,       0,       0,       0,       0,
/*  64  @    65  A    66  B    67  C    68  D    69  E    70  F    71  G  */
        0,       0,       0,       0,       0,       1,       0,       1,
/*  72  H    73  I    74  J    75  K    76  L    77  M    78  N    79  O  */
        0,       0,       0,       0,       0,       0,       0,       0,
/*  80  P    81  Q    82  R    83  S    84  T    85  U    86  V    87  W  */
        0,       0,       0,       0,       1,       0,       0,       0,
/*  88  X    89  Y    90  Z    91  [    92  \    93  ]    94  ^    95  _  */
        0,       0,       0,       0,       0,       0,       0,       0,
/*  96  `    97  a    98  b    99  c   100  d   101  e   102  f   103  g  */
        0,       0,       0,       0,       0,       0,       0,       0,
/* 104  h   105  i   106  j   107  k   108  l   109  m   110  n   111  o  */
        0,       0,       0,       0,       0,       0,       0,       0,
/* 112  p   113  q   114  r   115  s   116  t   117  u   118  v   119  w  */
        0,       0,       0,       0,       0,       0,       0,       0,
/* 120  x   121  y   122  z   123  {   124  |   125  }   126  ~   127 del */
        0,       0,       0,       0,       0,       0,       0,       0 
	};


enum parser_atom{
	 STATE_CMD,
	 STATE_K_LENGTH,
	 STATE_K,
	 STATE_V_LENGTH,
	 STATE_V,
	 LAST_STATE,
};

typedef int state_fn(struct request *req,char *sb);
state_fn state_cmd,state_next,last_state;

static state_fn *states[LAST_STATE+1]={
	[STATE_CMD]=&state_cmd,
	[STATE_K_LENGTH]=&state_next,
	[STATE_K]=&state_next,
	[STATE_V_LENGTH]=&state_next,
	[STATE_V]=&state_next,
	[LAST_STATE]=&last_state,
};

static int transforms[LAST_STATE]={
	[STATE_CMD]=STATE_K_LENGTH,
	[STATE_K_LENGTH]=STATE_K,
	[STATE_K]=STATE_V_LENGTH,
	[STATE_V_LENGTH]=STATE_V,
	[STATE_V]=LAST_STATE,
};

enum{
	STATE_CONTINUE,
	STATE_FAIL,
	STATE_DONE,
};

struct request *request_new()
{
	struct request *req;
	req=calloc(1,sizeof(struct request));
	return req;
}
int last_state(struct request *req,char *sb)
{
	return STATE_DONE;
}

int state_cmd(struct request *req,char *sb)
{
	int term=0;
	char c;
	int i=0;
	int pos=0;
	while(req->data[i]!='\0'){
		c=req->data[i++];
		pos++;
		switch(c){
			case '\r':
				term=1;
				break;
			case '\n':
				if(term){
					req->pos=pos;
					return STATE_CONTINUE;
				}
				else
					return STATE_FAIL;
			default:
				*sb=c;
				sb++;
				break;
			}
	}
	return STATE_FAIL;
}

int state_next(struct request *req,char *sb)
{
	int term=0;
	char c;
	int i=req->pos;
	int pos=i;
	while(req->data[i]!='\0'){
		c=req->data[i++];
		(pos)++;
		switch(c){
			case '\r':
				term=1;
				break;
				  
			case '\n':
				  if(term){
					req->pos=pos;
					return STATE_CONTINUE;
				}
				else
					return STATE_FAIL;
			default:
				*sb=c;
				sb++;
		}
	}

	return STATE_FAIL;
}

void request_parse(struct request *req)
{
	int state=STATE_CMD;
	state_fn *func=states[state];
	int ret;
	char sb[BUF_SIZE]={0};

	while((ret=func(req,sb))!=STATE_DONE){
		if(ret==STATE_FAIL){
			return;
		}
		switch(state){
			case STATE_CMD:
				req->cmd=strdup(sb);
				memset(sb,0,BUF_SIZE);
				break;
			case STATE_K_LENGTH:
				req->k_len=atoi(sb);
				memset(sb,0,BUF_SIZE);
				break;
			case STATE_K:
				req->k=strdup(sb);
				memset(sb,0,BUF_SIZE);
				break;
					
			case STATE_V_LENGTH:
				req->v_len=atoi(sb);
				memset(sb,0,BUF_SIZE);
				break;

			case STATE_V:
				req->v=strdup(sb);
				memset(sb,0,BUF_SIZE);
				break;
		}

		state=transforms[state];
		func=states[state];
	
	}
}

void request_dump(struct request *req)
{
	if(!req)
		return;

	printf("req-dump");
	if(req->cmd)
		printf(" -->cmd:<%s>;",req->cmd);

	printf("k_len:<%d>;",req->k_len);
	if(req->k)
		printf("key:<%s>;",req->k);
	printf("v_len:<%d>;",req->v_len);

	if(req->v){
		if(strlen(req->v)<1024)
			printf("v:<%s>",req->v);
		else
			printf("v is TOO LONG....TOO LONG.....");
	}
	printf("\n");
}

void request_free(struct request *req)
{
	if(req){
		if(req->cmd)
			free(req->cmd);
		if(req->k)
			free(req->k);
		if(req->v)
			free(req->v);

		free(req);
	}
}

 /*Copyright (c) 2011, BohuTANG <overred.shuttler at gmail dot com>
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
 *   * Neither the name of nessDB nor the names of its contributors may be used
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
#include "response.h"

struct response *response_new(int argc, STATUS status)
{
	struct response *res;
	res = calloc(1, sizeof(struct response));
	res->argv = calloc(argc, sizeof(char*));
	res->argc = argc;
	res->status = status;
	return res;
}


#define BUF_SIZE (10240)
void response_detch(struct response *res, char *ackbuf)
{
	int i;
	switch (res->status) {
		case OK:
			strcat(ackbuf,"+OK\r\n");
			break;

		case OK_200:
			break;

		case OK_404:
			strcat(ackbuf,"$-1\r\n");
			return;

		case OK_PONG:
			strcat(ackbuf,"+PONG\r\n");
			return;	

		case ERR:
			strcat(ackbuf,"-ERROR\r\n");
			return;;
	}
	if (res->argc > 1) {
		char buf[BUF_SIZE] = {0};
		snprintf(buf, sizeof buf, "*%d\r\n", res->argc);
		strcat(ackbuf, buf);
	}

	for (i = 0; i < res->argc; i++) {
		char ls[BUF_SIZE] = {0};
		if (res->argv[i] == NULL){
			strcat(ackbuf, "$-1\r\n");
		} else {
			int l = strlen(res->argv[i]);
			snprintf(ls, sizeof ls, "$%d\r\n", l);
			strcat(ackbuf, ls);

			strcat(ackbuf, res->argv[i]);
			strcat(ackbuf, "\r\n");
		}
	}
}

void response_dump(struct response *res)
{
	int i;
	if (!res)
		return;

	printf("response-dump--->{");
	printf("argc:<%d>\n", res->argc);
	for (i = 0; i < res->argc; i++) {
		printf("\t\targv[%d]:<%s>\n", i, res->argv[i]);
	}
	printf("}\n\n");
}

void response_free(struct response *res)
{
	if (res) {
		free(res->argv);
		free(res);
	}
}


/*
 * Copyright (c) 2011-2012, BohuTANG <overred.shuttler at gmail dot com>
 * All rights reserved.
 * Code is licensed with BSD. See COPYING.BSD file.
 *
 */
 
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "db.h"
#include "db_java.h"

struct nessdb *_db = NULL;

JNIEXPORT jint JNICALL Java_com_kv_NessDB_open(JNIEnv *jenv, jobject clazz, jstring jpath)
{
	(void) clazz;
	(void) bufsize;
	int ret = 0;

	if (_db) {
		fprintf(stderr, "DB has open...\n");
		return ret;
	}

	const char *path = (*jenv)->GetStringUTFChars(jenv, jpath, NULL);
	if (path == NULL) {
		fprintf(stderr, "...jpath is null\n");
		return 0;
	}

	_db = db_open(path, 1);
	if (_db)
		ret = 1;

	 (*jenv)->ReleaseStringUTFChars(jenv, jpath, path);  

	return ret;
}

JNIEXPORT jint JNICALL Java_com_kv_NessDB_set(JNIEnv *jenv, jobject clazz, jbyteArray jkey, jint jklen, jbyteArray jval, jint jvlen)
{
	(void)clazz;
	char *key, *val;
	struct slice sk, sv;
	int ret = 0;

	if (!_db) {
		fprintf(stderr, "set-->db is null, pls open first\n");
		return ret;
	}

	memset(&sk, 0, sizeof(struct slice));
	memset(&sv, 0, sizeof(struct slice));


	key = (char*)(*jenv)->GetByteArrayElements(jenv, jkey, 0);
	key[jklen] = 0;

	val = (char*)(*jenv)->GetByteArrayElements(jenv, jval, 0);
	val[jvlen] = 0;


	if (key == NULL || val == NULL)
		goto RET;


	sk.data = key;
	sk.len = jklen;

	sv.data = val;
	sv.len = jvlen;

	ret = db_add(_db, &sk, &sv);

RET:
	/* release */
	if (key) {
		(*jenv)->ReleaseByteArrayElements(jenv, jkey, (jbyte*)key, 0);
	}
	if (val){
		(*jenv)->ReleaseByteArrayElements(jenv, jval, (jbyte*)val, 0);
	}

	return ret;
}

JNIEXPORT jint JNICALL Java_com_kv_NessDB_get(JNIEnv *jenv, jobject clazz, jbyteArray jkey, jint jklen, jbyteArray jval)
{
	(void)clazz;
	int ret = 0;
	char *key;
	struct slice sk, sv;

	if (!_db) {
		fprintf(stderr, "get-->db is null, pls open first\n");
		return ret;
	}

	memset(&sk, 0, sizeof(struct slice));
	memset(&sv, 0, sizeof(struct slice));

	key = (char*)(*jenv)->GetByteArrayElements(jenv, jkey, 0);
	if (key == NULL)
		return ret;

	key[jklen] = 0;

	sk.data = key;
	sk.len = jklen;

	ret = db_get(_db, &sk, &sv);

	if (sv.len > 0) {
		 (*jenv)->SetByteArrayRegion(jenv, jval, 0, sv.len, (jbyte*)sv.data);
		 if (sv.data)
			 free(sv.data);
	}

	/* release */
	(*jenv)->ReleaseByteArrayElements(jenv, jkey, (jbyte*)key, 0);

	return ret;
}

JNIEXPORT void JNICALL Java_com_kv_NessDB_info(JNIEnv *jenv, jobject clazz, jbyteArray jval)
{
	(void)clazz;
	char *info;

	if (!_db) {
		fprintf(stderr, "info-->db is null, pls open first\n");
		return;
	}

	info = db_info(_db);
	int len = strlen(info);

	if (len > 0) {
		 (*jenv)->SetByteArrayRegion(jenv, jval, 0, len, (jbyte*)info);
	}
}

JNIEXPORT jint JNICALL Java_com_kv_NessDB_remove(JNIEnv *jenv, jobject clazz, jbyteArray jkey, jint jklen)
{
	(void)clazz;
	char *key;
	struct slice sk;

	if (!_db) {
		fprintf(stderr, "remove-->db is null, pls open first\n");
		return 0;
	}

	memset(&sk, 0, sizeof(struct slice));

	key = (char*)(*jenv)->GetByteArrayElements(jenv, jkey, 0);
	if (key == NULL)
		return 0;

	key[jklen] = 0;
	sk.data = key;
	sk.len = jklen;

	db_remove(_db, &sk);

	/* release */
	(*jenv)->ReleaseByteArrayElements(jenv, jkey, (jbyte*)key, 0);

	return 1;
}

JNIEXPORT void JNICALL Java_com_kv_NessDB_close(JNIEnv *jenv, jobject clazz)
{
	(void)jenv;
	(void)clazz;

	if (_db) {
		db_close(_db);
		_db = NULL;
	}
}

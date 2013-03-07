#include "../../engine/db.h"
#include "../../engine/debug.h"

#include "db_java.h"

JNIEXPORT jlong JNICALL Java_org_nessdb_DB_open(JNIEnv *jenv, jobject clazz, jstring jpath)
{
	(void) clazz;

    struct nessdb *_db = NULL;

	const char *path = (*jenv)->GetStringUTFChars(jenv, jpath, NULL);

	if (path == NULL) {
		__ERROR("...jpath is null");
		return 0;
	}

	_db = db_open(path);
	 (*jenv)->ReleaseStringUTFChars(jenv, jpath, path);  

	return (jlong)_db;
}

JNIEXPORT jint JNICALL Java_org_nessdb_DB_set(JNIEnv *jenv, jobject clazz, jlong ptr, 
													 jbyteArray jkey, jint jklen,
													 jbyteArray jval, jint jvlen)
{
	(void)clazz;
	char *key, *ktmp = NULL, *val, *vtmp = NULL;
	struct slice sk, sv;
	int ret = 0;

    struct nessdb *_db = (struct nessdb *)ptr;
	if (!_db) {
		__ERROR("set-->db is null, pls open first");

		return ret;
	}


	if (jklen >= NESSDB_MAX_KEY_SIZE) {
		__ERROR("key length too long...%d", jklen);

		return (-1);
	}

	if (jklen == 0 || jvlen == 0)
		return ret;

	memset(&sk, 0, sizeof(struct slice));
	memset(&sv, 0, sizeof(struct slice));


	key = (char*)(*jenv)->GetByteArrayElements(jenv, jkey, 0);
	val = (char*)(*jenv)->GetByteArrayElements(jenv, jval, 0);
	if (key == NULL || val == NULL)
		goto RET;

	ktmp = malloc(jklen + 1);
	memset(ktmp, 0, jklen + 1);
	memcpy(ktmp, key, jklen);

	vtmp = malloc(jvlen + 1);
	memset(vtmp, 0, jvlen + 1);
	memcpy(vtmp, val, jvlen);


	sk.data = ktmp;
	sk.len = jklen;

	sv.data = vtmp;
	sv.len = jvlen;

	ret = db_add(_db, &sk, &sv);

RET:
	/* release */
	if (key) {
		(*jenv)->ReleaseByteArrayElements(jenv, jkey, (jbyte*)key, JNI_COMMIT);
		if (ktmp)
			free(ktmp);
	}
	if (val){
		(*jenv)->ReleaseByteArrayElements(jenv, jval, (jbyte*)val, JNI_COMMIT);
		if (vtmp)
			free(vtmp);
	}

	return ret;
}

JNIEXPORT jbyteArray JNICALL Java_org_nessdb_DB_get(JNIEnv *jenv, jobject clazz, jlong ptr,
													 jbyteArray jkey, jint jklen
													 )
{
	(void)clazz;
	char *key, *ktmp;
	struct slice sk, sv;

    struct nessdb *_db = (struct nessdb *)ptr;
	if (!_db) {
		__ERROR("get-->db is null, pls open first");

		return NULL;
	}

	if (jklen >= NESSDB_MAX_KEY_SIZE) {
		__ERROR("key length too long...%d", jklen);
		return NULL;
	}

	memset(&sk, 0, sizeof(struct slice));
	memset(&sv, 0, sizeof(struct slice));

	key = (char*)(*jenv)->GetByteArrayElements(jenv, jkey, 0);
	if (key == NULL)
		return NULL;

	ktmp = malloc(jklen + 1);
	memset(ktmp, 0, jklen + 1);
	memcpy(ktmp, key, jklen);

	sk.data = ktmp;
	sk.len = jklen;

	db_get(_db, &sk, &sv);

    jbyteArray jval = NULL;
	if (sv.len > 0) {
        jval = (*jenv)->NewByteArray(jenv, sv.len);
        if (jval != NULL) {
            (*jenv)->SetByteArrayRegion(jenv, jval, 0, sv.len, (jbyte*)sv.data);
        } else {
            __ERROR("jenv new bytearray(%d) failed...", sv.len);
        }
	}

	/* release */
	if (key) {
		(*jenv)->ReleaseByteArrayElements(jenv, jkey, (jbyte*)key, 0);
		free(ktmp);
	}

    if (sv.data)
        db_free_data(sv.data);

	return jval;
}

JNIEXPORT jbyteArray JNICALL Java_org_nessdb_DB_stats(JNIEnv *jenv, jobject clazz, jlong ptr)
{
	(void)clazz;
	char buf[1024 * 10] = {0};
	struct slice stats;

    struct nessdb *_db = (struct nessdb *)ptr;
	if (!_db) {
		__ERROR("info-->db is null, pls open first\n");

		return NULL;
	}

	stats.len = 1024 * 10;
	stats.data = buf;

	db_stats(_db, &stats);

    jbyteArray jval = (*jenv)->NewByteArray(jenv, stats.len);
	(*jenv)->SetByteArrayRegion(jenv, jval, 0, stats.len, (jbyte*)stats.data);

    return jval;
}

JNIEXPORT jint JNICALL Java_org_nessdb_DB_remove(JNIEnv *jenv, jobject clazz, jlong ptr, 
														jbyteArray jkey, jint jklen)
{
	(void)clazz;
	char *key, *ktmp;
	struct slice sk;

    struct nessdb *_db = (struct nessdb *)ptr;
	if (!_db) {
		__ERROR("remove-->db is null, pls open first\n");

		return 0;
	}

	if (jklen >= NESSDB_MAX_KEY_SIZE) {
		__ERROR("key length too long...%d", jklen);

		return (-1);
	}

	if (jklen == 0)
		return 0;

	memset(&sk, 0, sizeof(struct slice));

	key = (char*)(*jenv)->GetByteArrayElements(jenv, jkey, 0);
	if (key == NULL)
		return 0;


	ktmp = malloc(jklen + 1);
	memset(ktmp, 0, jklen + 1);
	memcpy(ktmp, key, jklen);

	sk.data = ktmp;
	sk.len = jklen;

	db_remove(_db, &sk);

	/* release */
	if (key) {
		(*jenv)->ReleaseByteArrayElements(jenv, jkey, (jbyte*)key, 0);
		free(ktmp);
	}

	return 1;
}

JNIEXPORT void JNICALL Java_org_nessdb_DB_close(JNIEnv *jenv, jobject clazz, jlong ptr)
{
	(void)jenv;
	(void)clazz;

    struct nessdb *_db = (struct nessdb *)ptr;
	if (_db) {
		db_close(_db);
		_db = NULL;
	}
}

JNIEXPORT void JNICALL Java_org_nessdb_DB_shrink(JNIEnv *jenv, jobject clazz, jlong ptr)
{
	(void)jenv;
	(void)clazz;

    struct nessdb *_db = (struct nessdb *)ptr;
	if (_db) {
		db_shrink(_db);
		_db = NULL;
	}
}

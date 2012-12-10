#include "db.h"
#include "db_java.h"
#include "debug.h"

struct nessdb *_db = NULL;

JNIEXPORT jint JNICALL Java_com_kv_nessDB_open(JNIEnv *jenv, jobject clazz, jstring jpath, jlong bufsize)
{
	(void) clazz;
	(void) bufsize;
	int ret = 0;

	if (_db) {
		__ERROR("DB has open...");

		return ret;
	}

	const char *path = (*jenv)->GetStringUTFChars(jenv, jpath, NULL);

	if (path == NULL) {
		__ERROR("...jpath is null");
		return 0;
	}

	_db = db_open(path);
	if (_db)
		ret = 1;

	 (*jenv)->ReleaseStringUTFChars(jenv, jpath, path);  

	return ret;
}

JNIEXPORT jint JNICALL Java_com_kv_nessDB_set(JNIEnv *jenv, jobject clazz,
													 jbyteArray jkey, jint jklen,
													 jbyteArray jval, jint jvlen)
{
	(void)clazz;
	char *key, *ktmp = NULL, *val, *vtmp = NULL;
	struct slice sk, sv;
	int ret = 0;

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

JNIEXPORT jint JNICALL Java_com_kv_nessDB_get(JNIEnv *jenv, jobject clazz,
													 jbyteArray jkey, jint jklen,
													 jbyteArray jval, jint jblen)
{
	(void)clazz;
	int ret = 0;
	char *key, *ktmp;
	struct slice sk, sv;

	if (!_db) {
		__ERROR("get-->db is null, pls open first");

		return ret;
	}

	if (jklen >= NESSDB_MAX_KEY_SIZE) {
		__ERROR("key length too long...%d", jklen);

		return (-1);
	}

	if (jblen == 0)
		return 0;

	memset(&sk, 0, sizeof(struct slice));
	memset(&sv, 0, sizeof(struct slice));

	key = (char*)(*jenv)->GetByteArrayElements(jenv, jkey, 0);
	if (key == NULL)
		return ret;

	ktmp = malloc(jklen + 1);
	memset(ktmp, 0, jklen + 1);
	memcpy(ktmp, key, jklen);

	sk.data = ktmp;
	sk.len = jklen;

	ret = db_get(_db, &sk, &sv);

	if (sv.len > 0) {
		ret = jblen > sv.len ? sv.len: jblen;
		 (*jenv)->SetByteArrayRegion(jenv, jval, 0, ret, (jbyte*)sv.data);
		 if (sv.data)
			 db_free_data(sv.data);
	}

	/* release */
	if (key) {
		(*jenv)->ReleaseByteArrayElements(jenv, jkey, (jbyte*)key, 0);
		free(ktmp);
	}

	return ret;
}

JNIEXPORT void JNICALL Java_com_kv_nessDB_stats(JNIEnv *jenv, jobject clazz,
													   jbyteArray jval, jint jblen)
{
	(void)clazz;
	char buf[1024*10] = {0};
	struct slice stats;

	if (!_db) {
		__ERROR("info-->db is null, pls open first\n");

		return;
	}

	if (jblen <= 0)
		return;

	stats.len = jblen;
	stats.data = buf;
	db_stats(_db, &stats);

	(*jenv)->SetByteArrayRegion(jenv, jval, 0, jblen, (jbyte*)buf);
}

JNIEXPORT jint JNICALL Java_com_kv_nessDB_remove(JNIEnv *jenv, jobject clazz,
														jbyteArray jkey, jint jklen)
{
	(void)clazz;
	char *key, *ktmp;
	struct slice sk;

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

JNIEXPORT void JNICALL Java_com_kv_nessDB_close(JNIEnv *jenv, jobject clazz)
{
	(void)jenv;
	(void)clazz;

	if (_db) {
		db_close(_db);
		_db = NULL;
	}
}

#include "jni.h"

#ifndef	_DB_JAVA_
#define _DB_JAVA_

#ifdef __cplusplus
	extern "C" {
#endif
JNIEXPORT jlong JNICALL Java_org_nessdb_DB_open(JNIEnv *, jobject , jstring);
JNIEXPORT jint JNICALL Java_org_nessdb_DB_set(JNIEnv *, jobject, jlong, jbyteArray, jint, jbyteArray, jint);
JNIEXPORT jbyteArray JNICALL Java_org_nessdb_DB_get(JNIEnv *, jobject, jlong, jbyteArray, jint);
JNIEXPORT jint JNICALL Java_org_nessdb_DB_remove(JNIEnv *, jobject, jlong, jbyteArray, jint);
JNIEXPORT jbyteArray JNICALL Java_org_nessdb_DB_stats(JNIEnv *, jobject, jlong);
JNIEXPORT void JNICALL Java_org_nessdb_DB_close(JNIEnv *, jobject, jlong);

JNIEXPORT void JNICALL Java_org_nessdb_DB_shrink(JNIEnv *, jobject, jlong);

#ifdef __cplusplus
}
#endif

#endif


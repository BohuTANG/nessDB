#include "jni.h"

#ifndef	_DB_JAVA_
#define _DB_JAVA_

#ifdef __cplusplus
	extern "C" {
#endif
JNIEXPORT jint JNICALL Java_com_kv_nessDB_open(JNIEnv *, jobject , jstring, jlong);
JNIEXPORT jint JNICALL Java_com_kv_nessDB_set(JNIEnv *, jobject, jbyteArray, jint, jbyteArray, jint);
JNIEXPORT jint JNICALL Java_com_kv_nessDB_get(JNIEnv *, jobject, jbyteArray, jint, jbyteArray, jint);
JNIEXPORT jint JNICALL Java_com_kv_nessDB_remove(JNIEnv *, jobject, jbyteArray, jint);
JNIEXPORT void JNICALL Java_com_kv_nessDB_stats(JNIEnv *, jobject, jbyteArray, jint);
JNIEXPORT void JNICALL Java_com_kv_nessDB_close(JNIEnv *, jobject);

#ifdef __cplusplus
}
#endif

#endif


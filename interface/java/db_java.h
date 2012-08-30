#include "jni.h"

#ifndef	_DB_JAVA_
#define _DB_JAVA_

#ifdef __cplusplus
	extern "C" {
#endif
JNIEXPORT jint JNICALL Java_com_kv_NessDB_open(JNIEnv *, jobject , jstring, jlong);
JNIEXPORT jint JNICALL Java_com_kv_NessDB_set(JNIEnv *, jobject, jbyteArray, jint, jbyteArray, jint);
JNIEXPORT jint JNICALL Java_com_kv_NessDB_get(JNIEnv *, jobject, jbyteArray, jint, jbyteArray);
JNIEXPORT void JNICALL Java_com_kv_NessDB_info(JNIEnv *, jobject, jbyteArray);
JNIEXPORT jint JNICALL Java_com_kv_NessDB_remove(JNIEnv *, jobject, jbyteArray, jint);
JNIEXPORT void JNICALL Java_com_kv_NessDB_close(JNIEnv *, jobject);

#ifdef __cplusplus
}
#endif

#endif


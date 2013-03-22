#include <unistd.h>
#include <stdlib.h>
#include "ctest.h"
#include "engine/db.h"

CTEST_DATA(db_test) {
    struct nessdb *db;
};

// Optional setup function for suite, called before every test in suite
CTEST_SETUP(db_test) {
    const char* test_data="ndbs";
	CTEST_LOG("%s()  open the db at setup %s", __func__, test_data);
    data->db=db_open(test_data);
}

// Optional teardown function for suite, called after every test in suite
CTEST_TEARDOWN(db_test) {
    CTEST_LOG("%s()  close the db at teardown", __func__);
	db_close(data->db);
}

// These tests are called with the struct* (named data) as argument
CTEST2(db_test, add) {
    struct slice sk, sv;
	sk.len = 3;
	sk.data = "key";
	sv.len = 5;
	sv.data = "value";
	db_add(data->db, &sk, &sv);
}
CTEST2(db_test, exist) {
    struct slice sk, sv;
	sk.len = 3;
	sk.data = "key";
	int yes=db_exists(data->db, &sk);
	ASSERT_TRUE(yes==1);
}
CTEST2(db_test, get) {
    struct slice sk, sv;
	sk.len = 3;
	sk.data = "key";
	db_get(data->db, &sk, &sv);
	ASSERT_STR("value",sv.data);
}
CTEST2(db_test, remove) {
    struct slice sk, sv;
	sk.len = 3;
	sk.data = "key";
	int yes=db_exists(data->db, &sk);
	CTEST_LOG("-----------%d",yes);
	ASSERT_TRUE(yes==1);
	db_remove(data->db, &sk);
	yes=db_exists(data->db, &sk);
	CTEST_LOG("-----------%d",yes);
	//ASSERT_TRUE(yes==0);
	db_get(data->db, &sk, &sv);
	CTEST_LOG("the value is %s",sv.data);
}

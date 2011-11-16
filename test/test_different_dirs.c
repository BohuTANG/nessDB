#include "../nessDB/db.h"
#include <assert.h>

void test_db_operations(nessDB *db)
{
	assert(db_add(db, "a", "b"));
	assert(strcmp(db_get(db, "a"), "b") == 0);
	assert(db_exists(db, "a"));
	db_remove(db, "a");
	assert(!db_exists(db, "a"));
}

int main(void) {
	nessDB *db1 = db_init(1024*1024*2, "tmp/1");
	test_db_operations(db1);
	db_destroy(db1);
	nessDB *db2 = db_init(1024*1024*2, "tmp/2");
	test_db_operations(db2);
	db_destroy(db2);
	return 0;
}
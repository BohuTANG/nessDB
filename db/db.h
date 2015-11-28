/*
 * Copyright (c) 2012-2015 The nessDB Project Developers. All rights reserved.
 * Code is licensed with BSD.
 *
 */

#ifndef nessDB_DB_H_
#define nessDB_DB_H_

struct db_cursor {
	int valid;
	struct nessdb *db;
	struct msg key;
	struct msg val;
};

struct env *env_open(const char *home, uint32_t flags);
void env_close(struct env *e);

struct nessdb *db_open(struct env *e, const char *dbname);
int db_get(struct nessdb *db, struct msg *k, struct msg *v);
int db_set(struct nessdb *db, struct msg *k, struct msg *v);
int db_del(struct nessdb *db, struct msg *k);
int db_close(struct nessdb *db);

/* db_cursor */
struct db_cursor *db_cursor_new(struct nessdb *db);
void db_cursor_free(struct db_cursor *cursor);

int db_c_valid(struct db_cursor *cursor);
void db_c_first(struct db_cursor *cursor);
void db_c_last(struct db_cursor *cursor);
void db_c_next(struct db_cursor *cursor);
void db_c_prev(struct db_cursor *cursor);

int db_change_compress_method(struct nessdb *db, int method);

#endif /* nessDB_DB_H_ */

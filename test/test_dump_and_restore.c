#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../engine/db.h"

#define DB_BUFFER 2097152

int main(int argc,char** argv)
{
  int i, keys = 0;
  const char *basedir = "/data/nessDB";
  struct nessdb *db = db_open(DB_BUFFER, basedir);
  // Add a few lines in DB
	struct slice sk, sv;
  sk.len = 10;
  sv.len = 10;
  sk.data = "key#1";
  sv.data = "value#1";
  db_add(db, &sk, &sv);
  sk.data = "key#4";
  sv.data = "value#4";
  db_add(db, &sk, &sv);
  sk.data = "key#3";
  sv.data = "value#3";
  db_add(db, &sk, &sv);
  sk.data = "key#2";
  sv.data = "value#2";
  db_add(db, &sk, &sv);
  sk.data = "key#5";
  sv.data = "value#5";
  db_add(db, &sk, &sv);
  dbLine *myDb = db_get_all(db, &keys);
  db_drop(db);
  
  db = db_open(DB_BUFFER, basedir);
  for(i=0; i < keys; i++) {
    sk.data = myDb[i].key;
    sv.data = myDb[i].val;
    db_add(db, &sk, &sv);
    //printf("%d - key=%s - value=%s\n", (int) i, myDb[i].key, myDb[i].val );
  }
  db_close(db);
  
  free(myDb); 
  myDb = NULL;
  return 0;
}
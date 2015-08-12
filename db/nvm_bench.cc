#include <iostream>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>

#include "rocksdb/c.h"

#include <unistd.h>

using namespace std;

const char DBPath[] = "rocksdb";

int main(int argc, char **argv) {
  char key[8];
  char value[8];
  char *err = NULL;
  long cpus = sysconf(_SC_NPROCESSORS_ONLN);

  rocksdb_t *db;
  rocksdb_options_t *options;
  rocksdb_writeoptions_t *writeoptions;

  options = rocksdb_options_create();

  rocksdb_options_increase_parallelism(options, (int)(cpus));
  rocksdb_options_optimize_level_style_compaction(options, 0);
  rocksdb_options_set_create_if_missing(options, 1);

  db = rocksdb_open(options, DBPath, &err);
  if (err) {
    cout << "OPEN ERROR: " << err << endl << flush;
    return EXIT_FAILURE;
  }

  writeoptions = rocksdb_writeoptions_create();

  for (int i = 0; i < 1000; ++i) {
    sprintf(key, "k%06d", i);
    sprintf(value, "v%06d", i);

    rocksdb_put(db, writeoptions, key, strlen(key), value, strlen(value) + 1, &err);
    if (err) {
      cout << "WRITE ERROR: " << err << endl << flush;
      return EXIT_FAILURE;
    }
  }

  rocksdb_writeoptions_destroy(writeoptions);
  rocksdb_garbage_collect(db, &err);
  if (err) {
    cout << "GARBAGE COLLECTION FAILED " << err << endl << flush;
    return EXIT_FAILURE;
  }

  rocksdb_options_destroy(options);
  rocksdb_close(db);

  cout << "DONE\n" << flush;

  return EXIT_SUCCESS;
}

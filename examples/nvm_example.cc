#include <iostream>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>

#include "rocksdb/c.h"

#include <unistd.h>

using namespace std;

const char DBPath[] = "/home/raven/thesis/rocks_test";

int main(int argc, char **argv)
{
    const char key[] = "key";
    const char *value = "value";

    char *err = NULL;
    char *returned_value;

    size_t len;

    rocksdb_t *db;

    rocksdb_options_t *options;

    rocksdb_writeoptions_t *writeoptions;
    rocksdb_readoptions_t *readoptions;

    long cpus = sysconf(_SC_NPROCESSORS_ONLN);

    options = rocksdb_options_create();

    rocksdb_options_increase_parallelism(options, (int)(cpus));
    rocksdb_options_optimize_level_style_compaction(options, 0);

    rocksdb_options_set_create_if_missing(options, 1);

    db = rocksdb_open(options, DBPath, &err);
    if(err)
    {
	cout << "OPEN ERROR: " << err << endl << flush;
	return EXIT_FAILURE;
    }

    cout << "Database is open\n" << flush;

    writeoptions = rocksdb_writeoptions_create();

    cout << "PUT DATA " << key << ":" << value << endl << flush;

    rocksdb_put(db, writeoptions, key, strlen(key), value, strlen(value) + 1, &err);
    if(err)
    {
	cout << "WRITE ERROR: " << err << endl << flush;
	return EXIT_FAILURE;
    }

    readoptions = rocksdb_readoptions_create();

    returned_value = rocksdb_get(db, readoptions, key, strlen(key), &len, &err);
    if(err)
    {
	cout << "GET ERROR: " << err << endl << flush;
	return EXIT_FAILURE;
    }

    cout << "GOT DATA " << returned_value << endl << flush;

    if(strcmp(returned_value, "value") != 0)
    {
	cout << "INVALID DATA ERROR: " << err << endl << flush;
	return EXIT_FAILURE;
    }

    free(returned_value);

    rocksdb_writeoptions_destroy(writeoptions);
    rocksdb_readoptions_destroy(readoptions);
    rocksdb_options_destroy(options);
    rocksdb_close(db);

    cout << "DONE\n" << flush;

    return EXIT_SUCCESS;
}

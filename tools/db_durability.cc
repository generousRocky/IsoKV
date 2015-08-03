#include <iostream>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>

#include "rocksdb/c.h"

#include <unistd.h>
#include <cstdlib>

using namespace std;

const char DBPath[] = "rocksdb";

int main(int argc, char **argv)
{
    if(argc != 2)
    {
	cout << "number of keys?" << endl << flush;

	return 0;
    }

    unsigned int iterations;
    unsigned int read_checks = 0;
    unsigned int actual_reads = 0;

    if(sscanf(argv[1], "%u", &iterations) != 1)
    {
	cout << "invalid parameter" << argv[1] << endl << flush;

	return 0;
    }

    char key[1024];
    char value[1024];
    char check_value[1024];

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

    writeoptions = rocksdb_writeoptions_create();
    readoptions = rocksdb_readoptions_create();

    for(unsigned int i = 0; i < iterations; ++i)
    {
	sprintf(key, "k%u", i);
	sprintf(value, "v%u", i);

	rocksdb_put(db, writeoptions, key, strlen(key), value, strlen(value) + 1, &err);
	if(err)
	{
	    cout << "WRITE ERROR: " << err << endl << flush;
	    return EXIT_FAILURE;
	}

	int rnd_chance = rand();

	if(rnd_chance < RAND_MAX / 2) //~50% chance
	{
	    ++read_checks;

	    for(unsigned int j = 0; j < i; ++j)
	    {
		++actual_reads;

		sprintf(key, "k%u", j);

		returned_value = rocksdb_get(db, readoptions, key, strlen(key), &len, &err);
		if(err)
		{
		    cout << "GET ERROR: " << err << endl << flush;
		    return EXIT_FAILURE;
		}

		if(!returned_value)
		{
		    cout << "TEST FAILED AFTER " << i << " iterations " << read_checks << " read loops and " << j << " reads: VALUE MISSING" << endl << flush;
		    return EXIT_FAILURE;
		}

		sprintf(check_value, "v%u", j);

		if(strncmp(check_value, returned_value, len))
		{
		    cout << "TEST FAILED AFTER " << i << " iterations " << read_checks << " read loops and " << j << " reads: VALUE MISMATCH " << returned_value << " vs " << check_value << endl << flush;
		    return EXIT_FAILURE;
		}

		free(returned_value);
	    }
	}

	cout << i * 100 / iterations << "% done\n" << flush;
    }

    rocksdb_writeoptions_destroy(writeoptions);
    rocksdb_readoptions_destroy(readoptions);

    rocksdb_garbage_collect(db, &err);
    if(err)
    {
	cout << "GARBAGE COLLECTION FAILED " << err << endl << flush;
	return EXIT_FAILURE;
    }

    rocksdb_options_destroy(options);
    rocksdb_close(db);

    cout << "DONE\n" << flush;
    cout <<"Results:PASS\n" << flush;
    cout << "Keys written: " << iterations << endl << flush;
    cout << "Read sessions: " << read_checks << endl << flush;
    cout << "Key reads: " << actual_reads << endl << flush;

    return EXIT_SUCCESS;
}

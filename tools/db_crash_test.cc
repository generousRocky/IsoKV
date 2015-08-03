#include <iostream>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>

#include "rocksdb/c.h"
#include "rocksdb/utilities/backupable_db.h"

#include <unistd.h>
#include <cstdlib>

#define DO_BACKUPS	    1
#define DO_LOAD_AND_CHECK   2

using namespace std;

const char DBPath[] = "rocksdb";

int main(int argc, char **argv)
{
    if(argc != 2)
    {
	cout << "state?" << endl << flush;

	return 0;
    }

    unsigned int state;
    unsigned int read_checks = 0;

    if(sscanf(argv[1], "%u", &state) != 1)
    {
	cout << "invalid parameter" << argv[1] << endl << flush;

	return 0;
    }

    if(state != DO_BACKUPS && state != DO_LOAD_AND_CHECK)
    {
	cout << "invalid parameter" << state << endl << flush;

	return 0;
    }

    char key[1024];
    char value[1024];
    char check_value[1024];

    char *err = NULL;
    char *returned_value;

    rocksdb::DB *db_rep;

    rocksdb::Status s;

    rocksdb::BackupEngine* backup_engine;
    rocksdb::BackupEngineReadOnly *backup_engine_read_only;

    size_t len;

    rocksdb_t *db;

    rocksdb_options_t *options;

    options = rocksdb_options_create();

    long cpus = sysconf(_SC_NPROCESSORS_ONLN);

    rocksdb_options_increase_parallelism(options, (int)(cpus));
    rocksdb_options_optimize_level_style_compaction(options, 0);

    rocksdb_options_set_create_if_missing(options, 1);

    if(state == DO_BACKUPS)
    {
	rocksdb_writeoptions_t *writeoptions;

	db = rocksdb_open(options, DBPath, &err);
	if(err)
	{
	    cout << "OPEN ERROR: " << err << endl << flush;
	    return EXIT_FAILURE;
	}

	writeoptions = rocksdb_writeoptions_create();

	cout << "Writing keys\n" << flush;

	for(unsigned int i = 0; i < 100; ++i)
	{
	    sprintf(key, "k%u", i);
	    sprintf(value, "v%u", i);

	    rocksdb_put(db, writeoptions, key, strlen(key), value, strlen(value) + 1, &err);
	    if(err)
	    {
		cout << "WRITE ERROR: " << err << endl << flush;
		return EXIT_FAILURE;
	    }
	}

	cout << "Creating backup\n" << flush;

	db_rep = (rocksdb::DB *)rocksdb_get_rep(db);

	s = rocksdb::BackupEngine::Open(db_rep->GetEnv(), rocksdb::BackupableDBOptions("rocksdb_backup"), &backup_engine);

	if(!s.ok())
	{
	    cout << "Unable to backup db\n" << flush;
	}

	if(!backup_engine->CreateNewBackup(db_rep).ok())
	{
	    cout << "BACKUP SAVE ERROR\n" << flush;
	    return EXIT_FAILURE;
	}

	delete backup_engine;

	cout << "Altering keys\n" << flush;

	for(unsigned int i = 0; i < 100; ++i)
	{
	    sprintf(key, "k%u", i);
	    sprintf(value, "v%u", i + 1);

	    rocksdb_put(db, writeoptions, key, strlen(key), value, strlen(value) + 1, &err);
	    if(err)
	    {
		cout << "WRITE ERROR: " << err << endl << flush;
		return EXIT_FAILURE;
	    }
	}

	rocksdb_save_ftl_state(db, &err);
	if(err)
	{
	    cout << "FTL SAVE ERROR: " << err << endl << flush;
	    return EXIT_FAILURE;
	}

	cout << "Waiting for kill signal\n" << flush;

	while(true)
	{
	    sleep(1);
	}
    }

    rocksdb_readoptions_t *readoptions;

    readoptions = rocksdb_readoptions_create();

    db = rocksdb_open(options, DBPath, &err);
    if(err)
    {
	cout << "OPEN ERROR: " << err << endl << flush;
	return EXIT_FAILURE;
    }

    db_rep = (rocksdb::DB *)rocksdb_get_rep(db);

    s = rocksdb::BackupEngineReadOnly::Open(db_rep->GetEnv(), rocksdb::BackupableDBOptions("rocksdb_backup"), &backup_engine_read_only);

    if(!s.ok())
    {
	cout << "Unable to open backup db\n" << flush;
	return 1;
    }

    if(!backup_engine_read_only->RestoreDBFromLatestBackup(DBPath, DBPath).ok())
    {
	cout << "Unable to load backup db\n" << flush;
	return 1;
    }

    delete backup_engine_read_only;

    rocksdb_close(db);

    db = rocksdb_open(options, DBPath, &err);
    if(err)
    {
	cout << "OPEN ERROR: " << err << endl << flush;
	return EXIT_FAILURE;
    }

    for(unsigned int i = 0; i < 100; ++i)
    {
	sprintf(key, "k%u", i);

	returned_value = rocksdb_get(db, readoptions, key, strlen(key), &len, &err);
	if(err)
	{
	    cout << "GET ERROR: " << err << endl << flush;
	    return EXIT_FAILURE;
	}

	if(!returned_value)
	{
	    cout << "TEST FAILED AFTER " << i << " iterations " << i << " read loops: VALUE MISSING\n" << flush;
	    return EXIT_FAILURE;
	}

	sprintf(check_value, "v%u", i);

	if(strncmp(check_value, returned_value, len))
	{
	    cout << "TEST FAILED AFTER " << i << " iterations " << read_checks << " read loops and " << i << " reads: VALUE MISMATCH " << returned_value << " vs " << check_value << endl << flush;
	    return EXIT_FAILURE;
	}

	free(returned_value);
    }

    rocksdb_readoptions_destroy(readoptions);

    rocksdb_close(db);

    return EXIT_SUCCESS;
}

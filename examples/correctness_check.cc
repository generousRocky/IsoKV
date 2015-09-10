#include <iostream>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <dirent.h> 
#include <execinfo.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <cxxabi.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <pthread.h>
#include <atomic>

#include "rocksdb/c.h"

#include <unistd.h>

using namespace std;

const char DBPath[] = "rocksdb";

std::atomic<int> i(0);

void generate_kv(const int seq, char *key, char *value)
{
	const int ks = 1000000000;
	
	sprintf(key, "%09d", seq);
	sprintf(value, "%09d", ks - seq);
}

void *read_keys(void *arg)
{
	char *returned_value;
	char key[10];
	char value[10];
	char *err = nullptr;
	
	rocksdb_readoptions_t *readoptions;
	
	unsigned long int len;
	
	rocksdb_t *db = (rocksdb_t *)arg;
	
	readoptions = rocksdb_readoptions_create();
	
	cout << "STARTING READ\n" << flush;
	
	for(int j = 0; j < 2000; ++j)
	{
		cout << "GET J " << j << endl << flush;
		
		int crt_seq = rand() % i;
		
		generate_kv(crt_seq, key, value);
		
		returned_value = rocksdb_get(db, readoptions, key, strlen(key), &len, &err);
		
		if(err) {
			cout << "GET ERROR: " << j << err << endl << flush;
			exit(EXIT_FAILURE);
		}
	
		if(returned_value == nullptr)
		{
			cout << "COULD NOT GET KEY " << j << endl << flush;
			exit(EXIT_FAILURE);
		}
		
		if(len != strlen(value))
		{
			cout << "LEN MISMATCH " << j << endl << flush;
			exit(EXIT_FAILURE);
		}
		
		for(int k = 0; k < len; ++k)
		{
			if(returned_value[k] != value[k])
			{
				cout << "VALUE MISMATCH " << j << endl << flush;
				exit(EXIT_FAILURE);
			}
		}
		
		usleep(100);
			
		free(returned_value);
	}
	
	cout << "DONE READ\n" << flush;
}

int main() {
	rocksdb_t *db;
	pthread_t reader;
	
  char key[10];
  char value[10];

  char *err = NULL;
	
  rocksdb_options_t *options;
  rocksdb_writeoptions_t *writeoptions;

  long cpus = sysconf(_SC_NPROCESSORS_ONLN);
	
	options = rocksdb_options_create();

  rocksdb_options_increase_parallelism(options, (int)(cpus));
  rocksdb_options_optimize_level_style_compaction(options, 0);
  rocksdb_options_set_create_if_missing(options, 1);
	rocksdb_options_set_max_open_files(options, 3072); 
		
  db = rocksdb_open(options, DBPath, &err);
  if(err) {
    cout << "OPEN ERROR: " << err << endl << flush;
    return EXIT_FAILURE;
  }

  cout << "Database is open\n" << flush;

	writeoptions = rocksdb_writeoptions_create();
	
	pthread_create(&reader, NULL, read_keys, db);
	
	for(; i < 2000; ++i)
	{
		generate_kv(i, key, value);
		
		rocksdb_put(db, writeoptions, key, strlen(key), value, strlen(value), &err);
		
		if(err) {
			fprintf(stderr, "WRITE ERROR: %s", err);
			
			exit(EXIT_FAILURE);
		}
	}
	
	pthread_join(reader, nullptr);

  rocksdb_writeoptions_destroy(writeoptions);

  rocksdb_options_destroy(options);
  rocksdb_close(db);

  cout << "DONE WRITE\n" << flush;

  return EXIT_SUCCESS;
}

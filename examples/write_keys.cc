#include <iostream>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <dirent.h> 

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "rocksdb/c.h"

#include <unistd.h>

using namespace std;

const char DBPath[] = "./rocksdb";
const char chars[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz1234567890";

void write_time(const double duration, const int idx)
{
	static int crt_cnt = 0;
	int out_fd;
	string filename;
	string to_write;
	
	mkdir((string("./times/") + to_string(crt_cnt / 100)).c_str(), 0777);
	
	filename = string("./times/") + to_string(crt_cnt / 100) + string("/") + to_string(idx) + string(".txt");

	++crt_cnt;
	
	out_fd = open(filename.c_str(), O_CREAT | O_WRONLY, 0666);
	
	if(out_fd < 0)
	{
		perror("can not create file");
		exit(EXIT_FAILURE);
	}
	
	to_write = to_string(idx) + string(":") + to_string(duration);
	
	if(write(out_fd, to_write.c_str(), to_write.length()) < 0)
	{
		
	}
	
	close(out_fd);
}

int check_directory(const int idx)
{
  DIR           *d;
  struct dirent *dir;
	struct stat buf;
	
	int exists;
	int out_fd;
	
	static int crt_cnt = 0;
	
	string filename;
	string to_write;
	
	mkdir((string("./sizes/") + to_string(crt_cnt / 100)).c_str(), 0777);
	
	filename = string("./sizes/") + to_string(crt_cnt / 100) + string("/") + to_string(idx) + string(".txt");

	++crt_cnt;
	
	out_fd = open(filename.c_str(), O_CREAT | O_WRONLY, 0666);
	
	if(out_fd < 0)
	{
		perror("can not create file");
		exit(EXIT_FAILURE);
	}
			
  d = opendir("./rocksdb");
  if (d)
  {
    while ((dir = readdir(d)) != NULL)
    {			
			if(strcmp(dir->d_name, ".") == 0)
			{
				continue;
			}
			
			if(strcmp(dir->d_name, "..") == 0)
			{
				continue;
			}
			
			exists = stat((string("./rocksdb/") + string(dir->d_name)).c_str(), &buf);
			if (exists >= 0) {
				to_write = string(dir->d_name) + string(":") + to_string(buf.st_size) + string("\n");
				if(write(out_fd, to_write.c_str(), to_write.length()) < 0)
				{
					//error
				}
			}
			else
			{
				printf("can not stat %s", (string("./rocksdb/") + string(dir->d_name)).c_str());
			}
    }

    closedir(d);
  }
	
	close(out_fd);

  return(0);
}

void generate_random(char *data, const int len)
{
	int i;
	
	for(i = 0; i < len; ++i)
	{
		data[i] = chars[rand() % strlen(chars)];
	}
	
	data[len] = '\0';
}

int main() {
  char key[10];
  char value[10];
	
	unsigned int i = 0;

  char *err = NULL;

  rocksdb_t *db;
  rocksdb_options_t *options;
  rocksdb_writeoptions_t *writeoptions;

  long cpus = sysconf(_SC_NPROCESSORS_ONLN);
	
	options = rocksdb_options_create();

  rocksdb_options_increase_parallelism(options, (int)(cpus));
  rocksdb_options_optimize_level_style_compaction(options, 0);
  rocksdb_options_set_create_if_missing(options, 1);
	rocksdb_options_set_max_open_files(options, 3072); 
	
	mkdir("./sizes", 0777);
	mkdir("./times", 0777);
	
  db = rocksdb_open(options, DBPath, &err);
  if(err) {
    cout << "OPEN ERROR: " << err << endl << flush;
    return EXIT_FAILURE;
  }

  cout << "Database is open\n" << flush;

	writeoptions = rocksdb_writeoptions_create();
	
	for(; i < 1000000; ++i)
	{
		generate_random(key, 9);
		generate_random(value, 9);
		
		check_directory(i);
		
		clock_t startTime = clock();
		
		rocksdb_put(db, writeoptions, key, strlen(key), value, strlen(value),
                                                                        &err);
		
		write_time(double( clock() - startTime ) / ((double)CLOCKS_PER_SEC / 1000), i);
		
		fprintf(stderr, "%.02lf %% done\r", (double)i / (double)10000);
		fflush(stderr);
		if(err) {
			fprintf(stderr, "\nWRITE ERROR: %s", err);
			
			exit(EXIT_FAILURE);
		}
	}

  rocksdb_writeoptions_destroy(writeoptions);

  rocksdb_options_destroy(options);
  rocksdb_close(db);

  cout << "DONE\n" << flush;

  return EXIT_SUCCESS;
}

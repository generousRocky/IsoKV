#include <iostream>
#include <sstream>
#include <cassert>

#include "rocksdb/db.h"
#include "rocksdb/utilities/object_registry.h"
#include "env_nvm.h"
#include "args.h"

using namespace rocksdb;

int main(int argc, char *argv[]) {

  std::unique_ptr<Env> env_guard;
  Env *env = NewCustomObject<Env>("nvm://punits:0-127@nvme0n1/opt/rtest/nvm.meta", &env_guard);
  assert(env);

  Options options;
  options.env = env;
  options.compression = rocksdb::kNoCompression;
  options.IncreaseParallelism();
  options.create_if_missing = true;

  DB* db;
  Status s = DB::Open(options, "/opt/rtest/db", &db);
  std::cout << s.ToString() << std::endl;
  assert(s.ok());

  delete db;

  return 0;
}


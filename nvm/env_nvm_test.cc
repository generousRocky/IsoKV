#include <iostream>
#include <sstream>

#include "rocksdb/db.h"
#include "rocksdb/utilities/env_registry.h"
#include "env_nvm.h"

using namespace rocksdb;

std::string kDBPath = "/tmp/testdb";

void bulk_insert(int nruns, int nkvpairs) {
  DB* db;

  std::unique_ptr<Env> env_guard;
  Env *env = NewEnvFromUri("nvm://nvme0n1", &env_guard);
  assert(env);

  Options options;
  options.env = env;
  options.compression = rocksdb::kNoCompression;
  options.IncreaseParallelism();
  options.create_if_missing = true;

  Status s = DB::Open(options, kDBPath, &db);
  std::cout << s.ToString() << std::endl;
  assert(s.ok());

  s = db->Put(WriteOptions(), "key1", "value");
  assert(s.ok());
  std::string value;

  s = db->Get(ReadOptions(), "key1", &value);
  assert(s.ok());
  assert(value == "value");

  s = db->Delete(WriteOptions(), "key1");
  assert(s.ok());

  s = db->Get(ReadOptions(), "key", &value);
  assert(s.IsNotFound());

  for (int run = 0; run < nruns; ++run) {
    {                                     // BULK INSERT
      WriteBatch batch;
      for (int i = 0; i < nkvpairs; ++i) {
        std::stringstream key, val;

        key << "key{" << i << "}";
        val << "value{" << i << "}";

        batch.Put(key.str(), val.str());
      }

      s = db->Write(WriteOptions(), &batch);
      assert(s.ok());
    }

    for (int i = 0; i < nkvpairs; ++i) {  // GET
      std::string actual_val;
      std::stringstream key, expected_val;

      key << "key{" << i << "}";
      expected_val << "value{" << i << "}";

      s = db->Get(ReadOptions(), key.str(), &actual_val);

      assert(s.ok());
      assert(!expected_val.str().compare(actual_val));
    }

    {
      WriteBatch batch;
      for (int i = 0; i < nkvpairs; ++i) {
        std::stringstream key;

        key << "key{" << i << "}";
        batch.Delete(key.str());
      }

      s = db->Write(WriteOptions(), &batch);
      assert(s.ok());
    }
  }

  delete db;
}

int main() {

  bulk_insert(5, 200000);

  return 0;
}


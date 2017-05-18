#include <iostream>
#include <sstream>
#include <cassert>

#include "rocksdb/db.h"
#include "rocksdb/utilities/object_registry.h"
#include "env_nvm.h"
#include "args.h"

using namespace rocksdb;

Status bulk_insert(DB* db, WriteOptions& woptions, int nkvpairs) {
  Status s;
  WriteBatch batch;

  for (int i = 0; i < nkvpairs; ++i) {
    std::string key("k{" + std::to_string(i) + "}");
    std::string val("v{" + std::to_string(i) + "}");

    batch.Put(key, val);
  }

  s = db->Write(woptions, &batch);
  assert(s.ok());

  return s;
}

Status bulk_delete(DB* db, WriteOptions& woptions, int nkvpairs) {
  Status s;

  WriteBatch batch;
  for (int i = 0; i < nkvpairs; ++i) {
    std::string key("k{" + std::to_string(i) + "}");

    batch.Delete(key);
  }

  s = db->Write(woptions, &batch);
  assert(s.ok());

  return s;
}

// Gets and verifies that all values are as expected.
// NOT: Not bulk in the sense that it uses the "batch" feature.
Status bulk_get(DB* db, ReadOptions& roptions, int nkvpairs) {
  Status s;

  for (int i = 0; i < nkvpairs; ++i) {  // GET
    std::string key("k{" + std::to_string(i) + "}");
    std::string val("v{" + std::to_string(i) + "}");
    std::string returned;

    s = db->Get(roptions, key, &returned);
    assert(s.ok());
    assert(returned.compare(val)==0);

    if (!s.ok())
      break;
  }

  return s;
}

int main(int argc, char *argv[]) {

  Args args(argc, argv, {"nruns", "nkvs", "do_delete", "disableWAL"});
  if (argc < 2) {
    args.pr_usage();
    return -1;
  }
  args.pr_args();

  std::unique_ptr<Env> env_guard;
  Env *env = NewCustomObject<Env>("nvm://punits:0-127@nvme0n1/opt/rocks/nvm.meta", &env_guard);
  assert(env);

  Options options;
  options.env = env;
  options.compression = rocksdb::kNoCompression;
  options.IncreaseParallelism();
  options.create_if_missing = true;

  WriteOptions woptions;
  woptions.disableWAL = args["disableWAL"];

  ReadOptions roptions;

  DB* db;
  Status s = DB::Open(options, "/opt/rocks/test", &db);
  std::cout << s.ToString() << std::endl;
  assert(s.ok());

  for (int i = 0; i < args["nruns"]; ++i) {
    Status s;
    s = bulk_insert(db, woptions, args["nkvs"]);
    s = bulk_get(db, roptions, args["nkvs"]);
    if (args["do_delete"])
      s = bulk_delete(db, woptions, args["nkvs"]);
  }

  delete db;

  args.pr_args();

  return 0;
}


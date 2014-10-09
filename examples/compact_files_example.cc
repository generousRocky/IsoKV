// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
// An example showing how to use CompactFiles and EventListener API
// to do compactions.

#include <mutex>
#include <string>

#include "rocksdb/db.h"
#include "rocksdb/options.h"

using namespace rocksdb;
std::string kDBPath = "/tmp/rocksdb_compact_files_example";

// A simple compaction algorithm that always compacts everything
// to the highest level whenever possible.
class FullCompactor : public EventListener {
 public:
  explicit FullCompactor(const Options options) : options_(options) {
    compact_options_.compression = options_.compression;
    compact_options_.output_file_size_limit =
        options_.target_file_size_base;
  }

  void OnFlushCompleted(
      DB* db, const std::string& cf_name,
      const std::string& file_path,
      bool triggered_writes_slowdown,
      bool triggered_writes_stop) override {
    PickCompaction(db, cf_name, triggered_writes_stop);
  }

  void OnWriteStall(
      DB* db, const std::string& cf_name,
      WriteStallReason reason) override {
    if (reason == kWriteStallLevel0Stop) {
      printf("Enocuntered Write Stop\n");
      PickCompaction(db, cf_name, true);
    } else if (reason == kWriteStallLevel0SlowDown) {
        printf("Enocuntered Write Slow Down\n");
        PickCompaction(db, cf_name, false);
    }
  }

  void OnBackgroundCompactFilesCompleted(
      DB* db, const std::string& cf_name,
      const std::string job_id, const Status s) override {
    printf("CompactFiles Job %s terminated with status %s\n",
        job_id.c_str(), s.ToString().c_str());
    if (!s.ok() && retry_job_id == job_id) {
      printf("Retrying job %s\n", job_id.c_str());
      retry_mutex_.lock();
      retry_job_id = "";
      retry_mutex_.unlock();
      PickCompaction(db, cf_name, true);
    }
  }

 protected:
  void PickCompaction(DB* db, const std::string& cf_name,
                      bool retry_on_fail) {
    ColumnFamilyMetaData cf_meta;
    db->GetColumnFamilyMetaData(&cf_meta, cf_name);

    std::vector<uint64_t> input_file_numbers;
    for (auto level : cf_meta.levels) {
      for (auto file : level.files) {
        input_file_numbers.push_back(file.file_number);
      }
    }

    std::string job_id;
    Status s = db->ScheduleCompactFiles(
        &job_id, compact_options_,
        cf_name, input_file_numbers,
        options_.num_levels - 1);
    if (!s.ok()) {
      printf("CompactFiles fails -- %s\n", s.ToString().c_str());
    }
    // In some cases such as write-stall, it is desirable to retry
    // a compaction.  If retry_on_fail is on, then we will track
    // the submitted compaction job and retry when necessary.
    if (retry_on_fail) {
      retry_mutex_.lock();
      if (retry_job_id == "") {
        retry_job_id = job_id;
      }
      retry_mutex_.unlock();
    }
  }

 private:
  Options options_;
  CompactionOptions compact_options_;

  // variables for handling retry.
  std::mutex retry_mutex_;
  std::string retry_job_id;
};

int main() {
  Options options;
  options.create_if_missing = true;
  // Disable RocksDB background compaction.
  options.compaction_style = kCompactionStyleNone;
  // small block size for experiment.
  options.target_file_size_base = 10000;
  options.level0_slowdown_writes_trigger = 3;
  options.level0_stop_writes_trigger = 5;

  DB* db = nullptr;

  Status s = DB::Open(options, kDBPath, &db);
  assert(s.ok());
  assert(db);

  FullCompactor compactor(options);
  db->AddListener(&compactor);

  for (int i = 1000; i < 99999; ++i) {
    db->Put(WriteOptions(), std::to_string(i),
                            std::string(500, 'a' + (i % 26)));
  }

  std::string value;
  for (int i = 1000; i < 99999; ++i) {
    db->Get(ReadOptions(), std::to_string(i),
                           &value);
    assert(value == std::string(500, 'a' + (i % 26)));
  }
  db->RemoveListener(&compactor);

  delete db;
  DestroyDB(kDBPath, options);

  return 0;
}

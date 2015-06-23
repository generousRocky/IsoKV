//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#pragma once

#include <stdint.h>
#include <deque>
#include <limits>
#include "rocksdb/status.h"
#include "db/write_batch_internal.h"
#include "util/autovector.h"
#include "port/port.h"
#include "util/instrumented_mutex.h"

namespace rocksdb {

class WriteThread {
 public:
  static const uint64_t kNoTimeOut = std::numeric_limits<uint64_t>::max();
  // Information kept for every waiting writer
  struct Writer {
    Status status;
    WriteBatch* batch;
    bool sync;
    bool disableWAL;
    bool in_batch_group;
    bool done;
    int parallel_execute_id;
    bool has_callback;
    uint64_t timeout_hint_us;
    InstrumentedCondVar cv;

    explicit Writer(InstrumentedMutex* mu)
        : batch(nullptr),
          sync(false),
          disableWAL(false),
          in_batch_group(false),
          done(false),
          parallel_execute_id(-1),
          has_callback(false),
          timeout_hint_us(kNoTimeOut),
          cv(mu) {}
  };

  WriteThread() : unfinished_threads_(0) {}
  ~WriteThread() = default;

  // Before applying write operation (such as DBImpl::Write, DBImpl::Flush)
  // thread should grab the mutex_ and be the first on writers queue.
  // EnterWriteThread is used for it.
  // Be aware! Writer's job can be done by other thread (see DBImpl::Write
  // for examples), so check it via w.done before applying changes.
  //
  // Writer* w:                writer to be placed in the queue
  // uint64_t expiration_time: maximum time to be in the queue
  // See also: ExitWriteThread
  // REQUIRES: db mutex held
  Status EnterWriteThread(Writer* w, uint64_t expiration_time);

  // After doing write job, we need to remove already used writers from
  // writers_ queue and notify head of the queue about it.
  // ExitWriteThread is used for this.
  //
  // Writer* w:           Writer, that was added by EnterWriteThread function
  // Writer* last_writer: Since we can join a few Writers (as DBImpl::Write
  //                      does)
  //                      we should pass last_writer as a parameter to
  //                      ExitWriteThread
  //                      (if you don't touch other writers, just pass w)
  // Status status:       Status of write operation
  // See also: EnterWriteThread
  // REQUIRES: db mutex held
  void ExitWriteThread(Writer* w, Writer* last_writer, Status status);

  void StartParallelRun(WriteThread::Writer* w, uint32_t num_threads,
                        WriteThread::Writer* last_writer);

  bool ReportParallelRunFinish();

  void LeaderWaitEndParallel(WriteThread::Writer* self);

  void LeaderEndParallel(WriteThread::Writer* self,
                         WriteThread::Writer* last_writer);

  void EndParallelRun(WriteThread::Writer* w, bool need_wake_up_leader);

  // return total batch group size
  size_t BuildBatchGroup(Writer** last_writer,
                         autovector<WriteBatch*>* write_batch_group);

 private:
  // Queue of writers.
  std::deque<Writer*> writers_;
  std::vector<Writer*> parallel_writers_;
  std::atomic<uint32_t> unfinished_threads_;
};

}  // namespace rocksdb

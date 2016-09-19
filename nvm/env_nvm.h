#ifndef STORAGE_ROCKSDB_ENVNVM_H_
#define STORAGE_ROCKSDB_ENVNVM_H_

#include <fstream>
#include <map>
#include "rocksdb/env.h"
#include "util/threadpool.h"
#include "util/thread_local.h"
#include "util/thread_status_updater.h"

#ifndef __NVM_DEBUG_H
#define __NVM_DEBUG_H

#include <stdio.h>

#ifdef NVM_DEBUG_ENABLED
	#define NVM_DEBUG(x, ...) printf("%s:%s-%d: " x "", __FILE__, \
		__FUNCTION__, __LINE__, ##__VA_ARGS__);fflush(stdout);
#else
	#define NVM_DEBUG(x, ...)
#endif
#endif /* __NVM_DEBUG_H */

namespace rocksdb {

// Split given path into a pair of strings, first containing dirname,
// second containing filename
std::pair<std::string, std::string> SplitPath(const std::string& path);

class NVMFile;                  // Declared here, defined in envnvm_file.cc
class EnvNVM;                   // Declared here, defined here and in envnvm.cc
class NVMDirectory;             // Declared and defined here
class NVMSequentialFile;        // Declared and defined here
class NVMWritableFile;          // Declared and defined here
class NVMRandomAccessFile;      // Declared and defined here

class NVMFile {
public:
  NVMFile(void);
  explicit NVMFile(EnvNVM* env, const std::string& dname, const std::string& fname);

  bool UseOSBuffer() const;
  bool UseDirectIO() const;

  Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) const;
  Status Append(const Slice& data);
  Status PositionedAppend(const Slice& data, uint64_t offset);
  Status Truncate(uint64_t size);
  Status Close(void);
  Status Sync(void);
  Status Fsync(void);
  Status Flush(void);

  Status RangeSync(uint64_t offset, uint64_t nbytes);

  uint64_t ModifiedTime(void) const;

  uint64_t GetFileSize(void) const;
  uint64_t GetFileSize(void);

  uint64_t Size(void) const;

  size_t GetRequiredBufferAlignment(void) const;

  bool IsSyncThreadSafe() const;
  void SetIOPriority(Env::IOPriority pri);
  Env::IOPriority GetIOPriority();

  void SetPreallocationBlockSize(size_t size);
  void GetPreallocationStatus(size_t* block_size, size_t* last_allocated_block);
  size_t GetUniqueId(char* id, size_t max_size) const;
  Status InvalidateCache(size_t offset, size_t length);

  bool IsNamed(const std::string &name) const;
  const std::string& GetName(void) const;
  const std::string& GetDir(void) const;

  void Rename(const std::string& name);
  void PrepareWrite(size_t offset, size_t len);

  void Ref(void);
  void Unref(void);

  /*
   * Pre-allocate space for a file.
   * TODO: This used to be "protected", probably make the WritableFile etc.
   * friends of the class and keep it protected.
   */
  Status Allocate(uint64_t offset, uint64_t len);

private:
  // Private since only Unref() should be used to delete it.
  ~NVMFile();

  // No copying allowed.
  NVMFile(const NVMFile&);
  void operator=(const NVMFile&);

  EnvNVM* env_;
  std::string dname_;
  std::string fname_;

  uint64_t fsize_;

  char *buf_;
  uint64_t buf_len_;

  int refs_;
};

/**
 * Environment for non-volatile memory, specifically OpenChannelSSDs accessed
 * via liblightnvm.
 *
 * References:
 *  - include/rocksdb/env.h
 *  - include/rocksdb/utilities/env_librados.h
 *  - include/rocksdb/utilities/env_mirror.h
 *  - hdfs/env_hdfs.h
 */
class EnvNVM : public Env {
public:
  EnvNVM(const std::string& device);
  ~EnvNVM(void);

  // Create a logger
  //
  // On success, stores a pointer to a new Logger instance in *result
  // and returns OK. On failure stores nullptr in *result and returns non-OK.
  virtual Status NewLogger(
    const std::string& fname,
    shared_ptr<Logger>* result
  ) override;

  // Open a file with sequential read-only access.
  //
  // On success, stores a pointer to a new SequentialFile instance in *result
  // and returns OK. On failure stores nullptr in *result and returns non-OK.
  //
  // NOTE: Single-threaded access is assumed
  virtual Status NewSequentialFile(
    const std::string& fname,
    unique_ptr<SequentialFile>* result,
    const EnvOptions& options
  ) override;

  // Open a file with random read-only access to an existing file.
  //
  // On success, stores a pointer to a new RandomAccessFile instance in *result
  // and returns OK. On failure stores nullptr in *result and returns non-OK.
  //
  // NOTE: Multi-threaded access is assumed
  virtual Status NewRandomAccessFile(
    const std::string& fname,
    unique_ptr<RandomAccessFile>* result,
    const EnvOptions& options
  ) override;

  // Create and open a file with write-access
  //
  // On success, stores a pointer to a new WritableFile instance in *result and
  // returns OK.  On failure stores nullptr in *result and returns non-OK.
  //
  // NOTE: Single-threaded access is assumed.
  // NOTE: If a file with the same name already exists, then the existing file
  // is deleted prior to creation.
  virtual Status NewWritableFile(
    const std::string& fname,
    unique_ptr<WritableFile>* result,
    const EnvOptions& options
  ) override;

  // Rename and open a file with write-access
  //
  // On success, stores a pointer to a new WritableFile instance in *result and
  // returns OK. On failure stores nullptr in *result and returns non-OK.
  //
  // NOTE: Single-threaded access is assumed?
  // NOTE: If a file with the same name already exists, then the existing file
  // is deleted prior to creation.
  virtual Status ReuseWritableFile(
    const std::string& fname,
    const std::string& old_fname,
    unique_ptr<WritableFile>* result,
    const EnvOptions& options
  ) override;

  // Open a directory
  //
  // On success, stores a pointer to a new Directory instance in *result and
  // returns OK. On failure stores nullptr in *result and returns non-OK.
  virtual Status NewDirectory(
    const std::string& name,
    unique_ptr<Directory>* result
  ) override;

  // Returns OK if the named file exists.
  //         NotFound if the named file does not exist,
  //                  the calling process does not have permission to determine
  //                  whether this file exists, or if the path is invalid.
  //         IOError if an IO Error was encountered
  virtual Status FileExists(const std::string& fname) override;

  // Store in *result the names of the children of the specified directory.
  // The names are relative to "dir".
  // Original contents of *results are dropped.
  virtual Status GetChildren(
    const std::string& dname,
    std::vector<std::string>* result
  ) override;

  // Store in *result the attributes of the children of the specified directory.
  // In case the implementation lists the directory prior to iterating the files
  // and files are concurrently deleted, the deleted files will be omitted from
  // result.
  // The name attributes are relative to "dir".
  // Original contents of *results are dropped.
  virtual Status GetChildrenFileAttributes(
    const std::string& dir,
    std::vector<FileAttributes>* result
  ) override;

  // Delete the named file.
  virtual Status DeleteFile(const std::string& fname) override;

  // Create the specified directory. Returns error if directory exists.
  virtual Status CreateDir(const std::string& dirname) override;

  // Creates directory if it does not exist
  // Returns Ok if it exists, or successfully created. Non-OK otherwise.
  virtual Status CreateDirIfMissing(const std::string& dirname) override;

  // Delete the specified directory.
  virtual Status DeleteDir(const std::string& dirname) override;

  // Store the size of fname in *file_size.
  virtual Status GetFileSize(
    const std::string& fpath,
    uint64_t* fsize
  ) override;

  // Store the last modification time of fname in *file_mtime.
  virtual Status GetFileModificationTime(
    const std::string& fname,
    uint64_t* file_mtime) override;

  // Rename file src to target.
  virtual Status RenameFile(const std::string& src,
                            const std::string& target) override;

  // Hard Link file src to target.
  virtual Status LinkFile(
    const std::string& src,
    const std::string& target
  ) override {
    return Status::NotSupported("LinkFile is not supported for this Env");
  }

  // Lock the specified file.  Used to prevent concurrent access to
  // the same db by multiple processes.  On failure, stores nullptr in
  // *lock and returns non-OK.
  //
  // On success, stores a pointer to the object that represents the
  // acquired lock in *lock and returns OK.  The caller should call
  // UnlockFile(*lock) to release the lock.  If the process exits,
  // the lock will be automatically released.
  //
  // If somebody else already holds the lock, finishes immediately
  // with a failure.  I.e., this call does not wait for existing locks
  // to go away.
  //
  // May create the named file if it does not already exist.
  virtual Status LockFile(const std::string& fname, FileLock** lock) override;

  // Release the lock acquired by a previous successful call to LockFile.
  // REQUIRES: lock was returned by a successful LockFile() call
  // REQUIRES: lock has not already been unlocked.
  virtual Status UnlockFile(FileLock* lock) override;

    // Returns the number of micro-seconds since some fixed point in time. Only
  // useful for computing deltas of time.
  // However, it is often used as system time such as in GenericRateLimiter
  // and other places so a port needs to return system time in order to work.
  virtual uint64_t NowMicros(void) override;

  // Returns the number of nano-seconds since some fixed point in time. Only
  // useful for computing deltas of time in one run.
  // Default implementation simply relies on NowMicros
  virtual uint64_t NowNanos() override;

  // Get the number of seconds since the Epoch, 1970-01-01 00:00:00 (UTC).
  virtual Status GetCurrentTime(int64_t* unix_time) override;

  // Converts seconds-since-Jan-01-1970 to a printable string
  virtual std::string TimeToString(uint64_t time) override;

  // Get the current host name.
  virtual Status GetHostName(char* name, uint64_t len) override;

  // Get full directory name for this db.
  virtual Status GetAbsolutePath(
    const std::string& db_path,
    std::string* output_path
  ) override;

  // Generates a unique id that can be used to identify a db
  virtual std::string GenerateUniqueId(void) override;

  // *path is set to a temporary directory that can be used for testing. It may
  // or many not have just been created. The directory may or may not differ
  // between runs of the same process, but subsequent calls will return the
  // same directory.
  virtual Status GetTestDirectory(std::string* path) override;

  // ADDITIONS the standard Env interface


  Status AddFile(NVMFile* file);

  NVMFile* FindFile(const std::string& path);
  NVMFile* FindFile(const std::string& dname, const std::string& fname);

  Status RemoveFile(const std::string& path);
  Status RemoveFile(const std::string& dname, const std::string& fname);



  // REMOVE THIS STUFF LET SOME OTHER ENV MANAGE THAT IT

  // Arrange to run "(*function)(arg)" once in a background thread, in
  // the thread pool specified by pri. By default, jobs go to the 'LOW'
  // priority thread pool.

  // "function" may run in an unspecified thread.  Multiple functions
  // added to the same Env may run concurrently in different threads.
  // I.e., the caller may not assume that background work items are
  // serialized.
  // When the UnSchedule function is called, the unschedFunction
  // registered at the time of Schedule is invoked with arg as a parameter.
  virtual void Schedule(
    void (*function)(void* arg),
    void* arg,
    Priority pri = LOW,
    void* tag = nullptr,
    void (*unschedFunction)(void* arg) = 0
  ) override;

  // Arrange to remove jobs for given arg from the queue_ if they are not
  // already scheduled. Caller is expected to have exclusive lock on arg.
  virtual int UnSchedule(void* arg, Priority pri) override;

  // Start a new thread, invoking "function(arg)" within the new thread.
  // When "function(arg)" returns, the thread will be destroyed.
  virtual void StartThread(void (*function)(void* arg), void* arg) override;

  // Wait for all threads started by StartThread to terminate.
  virtual void WaitForJoin(void) override;

  // Get thread pool queue length for specific thrad pool.
  virtual unsigned int GetThreadPoolQueueLen(Priority pri = LOW) const override;

  // The number of background worker threads of a specific thread pool
  // for this environment. 'LOW' is the default pool.
  // default number: 1
  virtual void SetBackgroundThreads(int number, Priority pri = LOW) override;

  // Enlarge number of background worker threads of a specific thread pool
  // for this environment if it is smaller than specified. 'LOW' is the default
  // pool.
  virtual void IncBackgroundThreadsIfNeeded(int number, Priority pri) override;

  // Lower IO priority for threads from the specified pool.
  virtual void LowerThreadPoolIOPriority(Priority pool = LOW) override;

  // Sleep/delay the thread for the perscribed number of micro-seconds.
  virtual void SleepForMicroseconds(int micros) override;

  // Returns the status of all threads that belong to the current Env.
  virtual Status GetThreadList(
    std::vector<ThreadStatus>* thread_list
  ) override {
    return Status::NotSupported("Not supported.");
  }

  // Returns the pointer to ThreadStatusUpdater.  This function will be
  // used in RocksDB internally to update thread status and supports
  // GetThreadList().
  virtual ThreadStatusUpdater* GetThreadStatusUpdater() const override {
    return thread_status_updater_;
  }

  // Returns the ID of the current thread.
  virtual uint64_t GetThreadID() const override;

 private:
  // No copying allowed
  EnvNVM(const Env&);
  void operator=(const Env&);

  std::string device_;
  std::map<std::string, std::vector<NVMFile*>> fs_;

  std::vector<ThreadPool> thread_pools_;
  pthread_mutex_t mu_;
  std::vector<pthread_t> threads_to_join_;
};

// Directory object represents collection of files and implements
// filesystem operations that can be executed on directories.
class NVMDirectory : public Directory {
public:
  ~NVMDirectory(void) {}

  // Fsync directory. Can be called concurrently from multiple threads.
  virtual Status Fsync(void) override { return Status::OK(); }
};

class NVMLogger : public Logger {
public:
  explicit NVMLogger(
    const InfoLogLevel log_level = InfoLogLevel::INFO_LEVEL
  ) : Logger(log_level) {
    logf_ = fopen("/tmp/rks.log", "w");
  }

  ~NVMLogger(void) {
    fclose(logf_);
  }

  // Brings overloaded Logv()s into scope so they're not hidden when we override
  // a subset of them.
  using Logger::Logv;

  virtual void Logv(const char* format, va_list ap) override {
    vfprintf(logf_, format, ap);
    fprintf(logf_, "\n");
  }

  FILE *logf_;
};

class NVMSequentialFile : public SequentialFile {
public:
  NVMSequentialFile(void) : SequentialFile() {
    NVM_DEBUG("\n");
  }

  NVMSequentialFile(
    NVMFile *file, const EnvOptions& options
  ) : SequentialFile(), file_(file), pos_(0) {
    NVM_DEBUG("fname(%s), file(%p)\n", file_->GetName().c_str(), file_);

    file_->Ref();
  }

  ~NVMSequentialFile(void) {
    NVM_DEBUG("\n");

    file_->Unref();
  }

  Status Read(size_t n, Slice* result, char* scratch) {
    NVM_DEBUG("fname(%s), n(%lu), pos_(%lu), result(%p), scratch(%p)\n",
              file_->GetName().c_str(), n, pos_, result, scratch);

    if (pos_ >= file_->GetFileSize()) {
      *result = Slice();
      return Status::OK();
    }

    Status s = file_->Read(pos_, n, result, scratch);
    if (s.ok()) {
      pos_ += result->size();
    }

    return s;
  }

  Status Skip(uint64_t n) {
    NVM_DEBUG("fname(%s), n(%lu)\n", file_->GetName().c_str(), n);

    if (n + pos_ > file_->GetFileSize()) {      // TODO: Verify this boundary
      return Status::IOError("Skipping beyond end of file");
    }

    pos_ += n;

    return Status::OK();
  }

  Status InvalidateCache(size_t offset, size_t length) {
    NVM_DEBUG("Forwarding\n");

    return file_->InvalidateCache(offset, length);
  }

protected:
  NVMFile *file_;
  uint64_t pos_;
};

class NVMRandomAccessFile : public RandomAccessFile {
public:
  NVMRandomAccessFile(void) : RandomAccessFile() {
    NVM_DEBUG("\n");
  }

  NVMRandomAccessFile(
    NVMFile *file, const EnvOptions& options
  ) : RandomAccessFile(), file_(file) {
    NVM_DEBUG("fname(%s), file(%p)\n", file_->GetName().c_str(), file_);

    file_->Ref();
  }

  ~NVMRandomAccessFile(void) {
    NVM_DEBUG("\n");

    file_->Unref();
  }

  virtual Status Read(
    uint64_t offset,
    size_t n,
    Slice* result,
    char* scratch
  ) const override {
    NVM_DEBUG("offset(%lu), n(%lu), result(?), scratch(?)\n", offset, n);

    return file_->Read(offset, n, result, scratch);
  }

  virtual bool ShouldForwardRawRequest(void) const override {
    NVM_DEBUG("\n");

    return false;
  }

  virtual void EnableReadAhead(void) override {
    NVM_DEBUG("\n");
  }

  virtual size_t GetUniqueId(char* id, size_t max_size) const override {
    NVM_DEBUG("id(%s), max_size(%lu)\n", id, max_size);

    return file_->GetUniqueId(id, max_size);
  }

  virtual void Hint(AccessPattern pattern) override {
    NVM_DEBUG("pattern(%d)\n", pattern);
  }

  virtual Status InvalidateCache(size_t offset, size_t length) override {
    NVM_DEBUG("offset(%lu), length(%lu)\n", offset, length);

    return Status::IOError("InvalidateCache --> Not implemented.");
  }

protected:
  NVMFile *file_;
};

class NVMWritableFile : public WritableFile {
public:
  NVMWritableFile(void) : WritableFile() {
    NVM_DEBUG("\n");
  }

  NVMWritableFile(NVMFile *file, const EnvOptions& options
  ) : WritableFile(), file_(file) {
    NVM_DEBUG("file(%p), dname(%s), fname(%s), options(?)\n",
              file, file->GetDir().c_str(), file->GetName().c_str());

    file_->Ref();
  }

  ~NVMWritableFile(void) {
    NVM_DEBUG("\n");

    file_->Unref();
  }

  // Indicates if the class makes use of unbuffered I/O
  virtual bool UseOSBuffer() const override {
    NVM_DEBUG("forwarding\n");
    return file_->UseOSBuffer();
  }

  // This is needed when you want to allocate
  // AlignedBuffer for use with file I/O classes
  // Used for unbuffered file I/O when UseOSBuffer() returns false
  virtual size_t GetRequiredBufferAlignment(void) const override {
    NVM_DEBUG("forwarding\n");
    return file_->GetRequiredBufferAlignment();
  }

  virtual Status Append(const Slice& data) override {
    NVM_DEBUG("forwarding\n");
    return file_->Append(data);
  }

  // Positioned write for unbuffered access default forward to simple append as
  // most of the tests are buffered by default
  virtual Status PositionedAppend(const Slice& data, uint64_t offset) override {
    NVM_DEBUG("forwarding\n");
    return file_->PositionedAppend(data, offset);
  }

  // Truncate is necessary to trim the file to the correct size before closing.
  // It is not always possible to keep track of the file size due to whole pages
  // writes. The behavior is undefined if called with other writes to follow.
  virtual Status Truncate(uint64_t size) override {
    NVM_DEBUG("forwarding\n");
    return file_->Truncate(size);
  }

  virtual Status Close(void) override {
    NVM_DEBUG("forwarding\n");
    return file_->Close();
  }

  virtual Status Flush(void) override {
    NVM_DEBUG("forwarding\n");
    return file_->Flush();
  }

  // sync data
  virtual Status Sync(void) override {
    NVM_DEBUG("forwarding\n");
    return file_->Sync();
  }

  /*
   * Sync data and/or metadata as well.
   * By default, sync only data.
   * Override this method for environments where we need to sync
   * metadata as well.
   */
  virtual Status Fsync() override {
    NVM_DEBUG("forwarding\n");
    return file_->Fsync();
  }

  // true if Sync() and Fsync() are safe to call concurrently with Append()
  // and Flush().
  virtual bool IsSyncThreadSafe() const override {
    NVM_DEBUG("forwarding\n");
    return file_->IsSyncThreadSafe();
  }

  // Indicates the upper layers if the current WritableFile implementation
  // uses direct IO.
  virtual bool UseDirectIO() const override {
    NVM_DEBUG("forwarding\n");
    return file_->UseDirectIO();
  }

  /*
   * Change the priority in rate limiter if rate limiting is enabled.
   * If rate limiting is not enabled, this call has no effect.
   */
  virtual void SetIOPriority(Env::IOPriority pri) override {
    NVM_DEBUG("pri(%d)\n", pri);
  }

  virtual Env::IOPriority GetIOPriority(void) override {
    NVM_DEBUG("%d\n", io_priority_);
    return io_priority_;
  }

  /*
   * Get the size of valid data in the file.
   */
  virtual uint64_t GetFileSize() override {
    NVM_DEBUG("forwarding\n");
    return file_->GetFileSize();
  }

  // For documentation, refer to RandomAccessFile::GetUniqueId()
  virtual size_t GetUniqueId(char* id, size_t max_size) const override {
    NVM_DEBUG("forwarding\n");
    return file_->GetUniqueId(id, max_size);
  }

  // Remove any kind of caching of data from the offset to offset+length
  // of this file. If the length is 0, then it refers to the end of file.
  // If the system is not caching the file contents, then this is a noop.
  // This call has no effect on dirty pages in the cache.
  virtual Status InvalidateCache(size_t offset, size_t length) override {
    NVM_DEBUG("forwarding\n");
    return file_->InvalidateCache(offset, length);
  }

  // Sync a file range with disk.
  // offset is the starting byte of the file range to be synchronized.
  // nbytes specifies the length of the range to be synchronized.
  // This asks the OS to initiate flushing the cached data to disk,
  // without waiting for completion.
  // Default implementation does nothing.
  virtual Status RangeSync(uint64_t offset, uint64_t nbytes) override {
    NVM_DEBUG("forwarding\n");
    return file_->RangeSync(offset, nbytes);;
  }

  // PrepareWrite performs any necessary preparation for a write
  // before the write actually occurs.  This allows for pre-allocation
  // of space on devices where it can result in less file
  // fragmentation and/or less waste from over-zealous filesystem
  // pre-allocation.
  virtual void PrepareWrite(size_t offset, size_t len) override {
    NVM_DEBUG("forwarding\n");
    file_->PrepareWrite(offset, len);
  }

protected:
  /*
   * Pre-allocate space for a file.
   */
  virtual Status Allocate(uint64_t offset, uint64_t len) override {
    NVM_DEBUG("offset(%lu), len(%lu)\n", offset, len);

    return file_->Allocate(offset, len);
  }

 private:
  // No copying allowed
  NVMWritableFile(const WritableFile&);
  void operator=(const WritableFile&);

protected:
  NVMFile *file_;
};

}  // namespace rocksdb

#endif  // STORAGE_ROCKSDB_ENVNVM_H_

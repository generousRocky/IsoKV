#ifndef STORAGE_ROCKSDB_ENVNVM_H_
#define STORAGE_ROCKSDB_ENVNVM_H_

#include <iostream>
#include <sstream>
#include <fstream>
#include <iomanip>
#include <deque>
#include <map>
#include "rocksdb/env.h"
#include "include/rocksdb/threadpool.h"
#include "monitoring/thread_status_updater.h"
#include "util/thread_local.h"
#include "util/mutexlock.h"
#include "port/port.h"
#include <liblightnvm.h>

//#define NVM_DBG_ENABLED 1
#ifdef NVM_DBG_ENABLED

inline std::string methodName(const std::string& prettyFunction) {
  size_t begin = 0, end = 0;

  std::string prefix("rocksdb::");

  end = prettyFunction.find("(");
  if (end != std::string::npos) {
    begin = prettyFunction.rfind(prefix, end);
    begin += 9;
  }

  if ((begin < end) && (end < prettyFunction.length()))
    return prettyFunction.substr(begin, end-begin);

  return "";
}

#define __METHOD_NAME__ methodName(__PRETTY_FUNCTION__)
#define __FNAME__ (strrchr(__FILE__, '/') ? strrchr(__FILE__, '/') + 1 : __FILE__)
#define NVM_DBG(obj, x) do {                                            \
  std::stringstream ss; ss                                              \
  << std::setfill('-') << std::setw(15) << std::left << __FNAME__       \
  << std::setfill('-') << std::setw(5)  << std::right << __LINE__       \
  << std::setfill(' ') << std::setw(20)  << std::right << obj->txt()    \
  << std::setfill(' ') << " "                                           \
  << std::setfill(' ') << std::setw(34) << std::left << __METHOD_NAME__ \
  << std::setfill(' ') << " "                                           \
  << std::setfill(' ') << x                                             \
  << std::endl;                                                         \
  fprintf(stdout, "%s", ss.str().c_str()); fflush(stdout);              \
} while (0);
#else
#define NVM_DBG(obj, x)
#endif

namespace rocksdb {

static std::string kNvmMetaExt = ".meta";

// Splits a file path into a directory path, filename and determines whether the
// file is managed by EnvNVM. That is, whether it is write-ahead-log or sst
// file.
class FPathInfo {
public:
  FPathInfo(const std::string& fpath) {
    this->fpath(fpath);

    NVM_DBG(this, "");
  }

  FPathInfo(const FPathInfo& info) {
    this->fpath(info.fpath());

    NVM_DBG(this, "");
  }

  static bool ends_with(const std::string& subj, const std::string& suffix) {
    return subj.size() >= suffix.size() && std::equal(
      suffix.rbegin(), suffix.rend(), subj.rbegin()
    );
  }

  static const char sep = '/';

  void fpath(const std::string& fpath) {
    int sep_idx = fpath.find_last_of(sep, fpath.length());
    int fname_idx = sep_idx + 1;

    while(sep_idx > 0 && fpath[sep_idx] == sep) {
      --sep_idx;
    }

    fpath_ = fpath;
    dpath_ = fpath.substr(0, sep_idx + 1);
    fname_ = fpath.substr(fname_idx);

    nvm_managed_ = ends_with(fname_, "log") || \
                   ends_with(fname_, "sst") || \
                   ends_with(fname_, "ldb");
  }

  const std::string& fpath(void) const { return fpath_; }

  void dpath(const std::string& dpath) { fpath(dpath + std::string(1, sep) + fname_); }

  const std::string& dpath(void) const { return dpath_; }

  void fname(const std::string& fname) { fpath(dpath_ + std::string(1, sep) + fname); }

  const std::string& fname(void) const { return fname_; }

  bool nvm_managed(void) const { return nvm_managed_; }

  std::string txt(void) const {
    std::stringstream ss;

    ss << "dpath(" << dpath_ << "), "
       << "fname(" << fname_ << "), "
       << "nvm_managed(" << std::boolalpha << nvm_managed_ << ")";

    return ss.str();
  }

private:
  std::string fpath_;
  std::string dpath_;
  std::string fname_;
  bool nvm_managed_;
};

class EnvNVM;                   // Declared here, defined here and in envnvm.cc
class NvmStore;                 // Declared here, defined in env_nvm_store.cc
class NvmFile;                  // Declared here, defined in envnvm_file.cc
class NvmWritableFile;          // Declared here, defined here
class NvmSequentialFile;        // Declared here, defined here
class NvmRandomAccessFile;      // Declared here, defined here

enum BlkState {
  kFree = 0x1,
  kOpen = 0x2,
  kReserved = 0x4,
  kBad = 0x8,
};

//
// Stateful wrapper for provisioning of virtual blocks
class NvmStore {
public:
  NvmStore(EnvNVM* env,
           const std::string &dev_name,
           const std::vector<int> &punits,
           const std::string& mpath,
           size_t rate);

  ~NvmStore(void);

  struct nvm_vblk* get(void);
  struct nvm_vblk* get_reserved(size_t blk_idx);

  void put(struct nvm_vblk* blk);

  struct nvm_dev *GetDev(void) const { return dev_; }
  std::string GetDevName(void) const { return dev_name_; }
  std::string GetDevPath(void) const { return dev_path_; }

  size_t GetPunitCount(void) const { return punits_.size(); }

protected:

  Status recover(const std::string& mpath);

  Status persist(const std::string& mpath);

  std::string txt(void);

  EnvNVM* env_;
  std::string dev_name_;
  std::string dev_path_;
  struct nvm_dev *dev_;
  const struct nvm_geo *geo_;
  std::vector<struct nvm_addr> punits_;
  std::string mpath_;
  size_t rate_;

  port::Mutex mutex_;
  uint16_t curs_;
  std::deque<struct nvm_vblk*> reserved_;

  std::deque<std::pair<BlkState, struct nvm_vblk*>> blks_;
};

class NvmFile {
  friend class NvmWritableFile;

public:
  // Construct an NvmFile using nvm device associated with Env
  // if mpath exists, meta-data will be loaded from file via default-env
  NvmFile(EnvNVM* env, const FPathInfo& info, const std::string mpath);

  Status Allocate(uint64_t offset, uint64_t len);

  Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) const;
  Status Append(const Slice& data);
  Status PositionedAppend(const Slice& data, uint64_t offset);
  Status Truncate(uint64_t size);
  Status Close(void);
  Status Sync(void);
  Status RangeSync(uint64_t offset, uint64_t nbytes);
  Status Fsync(void);
  Status Flush(bool padded);
  Status Flush(void);

  void Rename(const std::string& fname);
  void PrepareWrite(size_t offset, size_t len);

  uint64_t ModifiedTime(void) const;

  uint64_t GetFileSize(void) const;

  size_t GetRequiredBufferAlignment(void) const;

  bool IsSyncThreadSafe() const;

  size_t GetUniqueId(char* id, size_t max_size) const;
  Status InvalidateCache(size_t offset, size_t length);

  bool IsNamed(const std::string &fname) const;
  const std::string& GetFname(void) const;
  const std::string& GetDpath(void) const;

  void Ref(void);
  void Unref(void);

  Status wmeta(void);
  std::string txt(void) const;

  port::Mutex read_mutex_;

private:
  ~NvmFile(void);               // Unref eventually deletes the object

  NvmFile(const NvmFile&);      // No copying allowed.
  void operator=(const NvmFile&);

  Status pad_last_block(void);

  EnvNVM* env_;
  port::Mutex refs_mutex_;
  int refs_;
  FPathInfo info_;
  uint64_t fsize_;
  std::string mpath_;

  size_t align_nbytes_;
  size_t stripe_nbytes_;
  size_t blk_nbytes_;

  char *buf_;
  size_t buf_nbytes_;
  size_t buf_nbytes_max_;

  std::deque<struct nvm_vblk*> blks_;

  size_t lu_bound_;
};

template<typename T>
std::string num_to_hex(T i, int width)
{
  std::stringstream stream;

  stream << "0x"
         << std::setfill ('0') << std::setw(width)
         << std::hex << i;

  return stream.str();
}

//
// Environment for non-volatile memory
// Specifically Open-Channel SSDs accessed via liblightnvm
//
class EnvNVM : public Env {
public:
  EnvNVM(const std::string& uri);
  ~EnvNVM(void);

  // Open (an existing) file with sequential read-only access
  //
  // On success, stores a pointer to a new SequentialFile instance in *result
  // and returns OK. On failure stores nullptr in *result and returns non-OK.
  //
  virtual Status NewSequentialFile(
    const std::string& fpath,
    unique_ptr<SequentialFile>* result,
    const EnvOptions& options
  ) override;

  // Open (an existing) file with random read-only access
  //
  // On success, stores a pointer to a new RandomAccessFile instance in *result
  // and returns OK. On failure stores nullptr in *result and returns non-OK.
  //
  virtual Status NewRandomAccessFile(
    const std::string& fpath,
    unique_ptr<RandomAccessFile>* result,
    const EnvOptions& options
  ) override;

  // Create and open a file with write-access
  //
  // On success, stores a pointer to a new WritableFile instance in *result and
  // returns OK.  On failure stores nullptr in *result and returns non-OK.
  //
  // NOTE: If a file with the same name already exists, then the existing file
  // is deleted prior to creation.
  virtual Status NewWritableFile(
    const std::string& fpath,
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
    const std::string& fpath,
    const std::string& old_fpath,
    unique_ptr<WritableFile>* result,
    const EnvOptions& options
  ) override;

  // Open a directory
  //
  // On success, stores a pointer to a new Directory instance in *result and
  // returns OK. On failure stores nullptr in *result and returns non-OK.
  virtual Status NewDirectory(
    const std::string& dpath,
    unique_ptr<Directory>* result
  ) override {
    NVM_DBG(this, "dpath(" << dpath << ")");
    Status s = posix_->NewDirectory(dpath, result);
    NVM_DBG(this, s.ToString());
    return s;
  }

  // Returns OK if the named file exists.
  //         NotFound if the named file does not exist,
  //                  the calling process does not have permission to determine
  //                  whether this file exists, or if the path is invalid.
  //         IOError if an IO Error was encountered
  virtual Status FileExists(const std::string& fpath) override;

  // Store in *result the names of the children of the specified directory.
  // The names are relative to "dir".
  // Original contents of *results are dropped.
  virtual Status GetChildren(
    const std::string& dpath,
    std::vector<std::string>* result
  ) override;

  // Store in *result the attributes of the children of the specified directory.
  // In case the implementation lists the directory prior to iterating the files
  // and files are concurrently deleted, the deleted files will be omitted from
  // result.
  // The name attributes are relative to "dir".
  // Original contents of *results are dropped.
  virtual Status GetChildrenFileAttributes(
    const std::string& dpath,
    std::vector<FileAttributes>* result
  ) override;

  // Store the size of fpath in *file_size.
  virtual Status GetFileSize(
    const std::string& fpath,
    uint64_t* fsize
  ) override;

  // Delete the named file.
  virtual Status DeleteFile(const std::string& fpath) override;

  // Create the specified directory. Returns error if directory exists.
  virtual Status CreateDir(const std::string& dpath) override {
    NVM_DBG(this, "delegating... dpath(" << dpath << ")");
    Status s = posix_->CreateDir(dpath);
    NVM_DBG(this, "Status(" << s.ToString() << ")");
    return s;
  }

  // Creates directory if it does not exist
  // Returns Ok if it exists, or successfully created. Non-OK otherwise.
  virtual Status CreateDirIfMissing(const std::string& dpath) override {
    NVM_DBG(this, "delegating... dpath(" << dpath << ")");
    Status s = posix_->CreateDirIfMissing(dpath);
    NVM_DBG(this, "Status(" << s.ToString() << ")");
    return s;
  }

  // Delete the specified directory.
  virtual Status DeleteDir(const std::string& dpath) override {
    NVM_DBG(this, "delegating... dpath(" << dpath << ")");
    Status s = posix_->DeleteDir(dpath);
    NVM_DBG(this, "Status(" << s.ToString() << ")");
    return s;
  }

  // Store the last modification time of fpath in *file_mtime.
  virtual Status GetFileModificationTime(
    const std::string& fpath,
    uint64_t* file_mtime) override;

  // Rename file fpath_src to fpath_tgt
  virtual Status RenameFile(const std::string& fpath_src,
                            const std::string& fpath_tgt) override;

  // Hard Link file at fpath_src to fpath_tgt
  virtual Status LinkFile(
    const std::string& fpath_src,
    const std::string& fpath_tgt
  ) override {
    return Status::NotSupported("LinkFile is not supported for this Env");
  }

  // PUBLIC ADDITIONS the environment interface - BEGIN
  std::string txt(void) const { return ""; }
  // PUBLIC ADDITIONS the environment interface - END

private:
  // PRIVATE ADDITIONS the Env interface - BEGIN
  NvmFile* FindFileUnguarded(const FPathInfo& info);

  Status DeleteFileUnguarded(const FPathInfo& info);
  // PRIVATE ADDITIONS the Env interface - END

//
// The following public methods are intercepted for debug tracing but otherwise
// entirely delegated to default environment.
//
public:
  // Create a logger
  //
  // On success, stores a pointer to a new Logger instance in *result
  // and returns OK. On failure stores nullptr in *result and returns non-OK.
  virtual Status NewLogger(
    const std::string& fpath, shared_ptr<Logger>* result
  ) {
    NVM_DBG(this, "delegating... fpath(" << fpath << ")");
    Status s = posix_->NewLogger(fpath, result);
    NVM_DBG(this, "Status(" << s.ToString() << ")");
    return s;
  }

  // Lock the specified file. Used to prevent concurrent access to the same db
  // by multiple processes. On failure, stores nullptr in *lock and returns
  // non-OK.
  //
  // On success, stores a pointer to the object that represents the acquired
  // lock in *lock and returns OK. The caller should call UnlockFile(*lock) to
  // release the lock. If the process exits, the lock will be automatically
  // released.
  //
  // If somebody else already holds the lock, finishes immediately with a
  // failure. I.e., this call does not wait for existing locks to go away.
  //
  // May create the named file if it does not already exist.
  virtual Status LockFile(const std::string& fpath, FileLock** lock) override {
    NVM_DBG(this, "delegating... fpath(" << fpath << ")");
    Status s = posix_->LockFile(fpath, lock);
    NVM_DBG(this, "Status(" << s.ToString() << ")");
    return s;
  }

  // Release the lock acquired by a previous successful call to LockFile.
  // REQUIRES: lock was returned by a successful LockFile() call
  // REQUIRES: lock has not already been unlocked.
  virtual Status UnlockFile(FileLock* lock) override {
    NVM_DBG(this, "lock(" << lock << ") -- delegating");
    Status s = posix_->UnlockFile(lock);
    NVM_DBG(this, "Status(" << s.ToString() << ")");
    return s;
  }

  // Returns the number of micro-seconds since some fixed point in time. Only
  // useful for computing deltas of time.
  // However, it is often used as system time such as in GenericRateLimiter
  // and other places so a port needs to return system time in order to work.
  virtual uint64_t NowMicros(void) override {
    /*NVM_DBG(this, "");*/
    return posix_->NowMicros();
  }

  // Returns the number of nano-seconds since some fixed point in time. Only
  // useful for computing deltas of time in one run.
  // Default implementation simply relies on NowMicros
  virtual uint64_t NowNanos(void) override {
    /*NVM_DBG(this, "");*/
    return posix_->NowNanos();
  }

  // Get the number of seconds since the Epoch, 1970-01-01 00:00:00 (UTC).
  virtual Status GetCurrentTime(int64_t* unix_time) override {
    NVM_DBG(this, "delegating...");
    Status s = posix_->GetCurrentTime(unix_time);
    NVM_DBG(this, "Status(" << s.ToString() << ")");
    return s;
  }

  // Converts seconds-since-Jan-01-1970 to a printable string
  virtual std::string TimeToString(uint64_t stamp) override {
    NVM_DBG(this, "delegating...  stamp(" << stamp << ")");
    std::string time = posix_->TimeToString(stamp);
    NVM_DBG(this, "time(" << time << ")");
    return time;
  }

  // Get the current host name.
  virtual Status GetHostName(char* name, uint64_t len) override {
    NVM_DBG(this, "delegating...");
    Status s = posix_->GetHostName(name, len);
    NVM_DBG(this, "Status(" << s.ToString() << ")");
    return s;
  }

  // Get full directory name for this db.
  virtual Status GetAbsolutePath(
    const std::string& db_path,
    std::string* output_path
  ) override {
    NVM_DBG(this, "delegating...");
    Status s = posix_->GetAbsolutePath(db_path, output_path);
    NVM_DBG(this, "Status(" << s.ToString() << ")");
    return s;
  }

  // *path is set to a temporary directory that can be used for testing. It may
  // or many not have just been created. The directory may or may not differ
  // between runs of the same process, but subsequent calls will return the
  // same directory.
  virtual Status GetTestDirectory(std::string* dpath) override {
    NVM_DBG(this, "delegating...");
    Status s = posix_->GetTestDirectory(dpath);
    NVM_DBG(this, "Status(" << s.ToString() << ")");
    return s;
  }

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
  ) override {
    NVM_DBG(this, "delegating...");
    posix_->Schedule(function, arg, pri, tag, unschedFunction);
  }

  // Arrange to remove jobs for given arg from the queue_ if they are not
  // already scheduled. Caller is expected to have exclusive lock on arg.
  virtual int UnSchedule(void* arg, Priority pri) override {
    NVM_DBG(this, "delegating...");
    return posix_->UnSchedule(arg, pri);
  }

  // Start a new thread, invoking "function(arg)" within the new thread.
  // When "function(arg)" returns, the thread will be destroyed.
  virtual void StartThread(void (*function)(void* arg), void* arg) override {
    NVM_DBG(this, "delegating...");
    posix_->StartThread(function ,arg);
  }

  // Wait for all threads started by StartThread to terminate.
  virtual void WaitForJoin(void) override {
    NVM_DBG(this, "delegating...");
    posix_->WaitForJoin();
  }

  // Get thread pool queue length for specific thrad pool.
  virtual unsigned int GetThreadPoolQueueLen(Priority pri = LOW) const override {
    NVM_DBG(this, "delegating...");
    return posix_->GetThreadPoolQueueLen(pri);
  }

  // The number of background worker threads of a specific thread pool
  // for this environment. 'LOW' is the default pool.
  // default number: 1
  virtual void SetBackgroundThreads(int number, Priority pri = LOW) override {
    NVM_DBG(this, "delegating... number(" <<number<< "), pri(" <<pri<< ")");
    posix_->SetBackgroundThreads(number, pri);
  }

  // Enlarge number of background worker threads of a specific thread pool
  // for this environment if it is smaller than specified. 'LOW' is the default
  // pool.
  virtual void IncBackgroundThreadsIfNeeded(int number, Priority pri) override {
    NVM_DBG(this, "delegating... number(" <<number<< ", pri(" <<pri<< ")");
    posix_->IncBackgroundThreadsIfNeeded(number, pri);
  }

  // Lower IO priority for threads from the specified pool.
  virtual void LowerThreadPoolIOPriority(Priority pool = LOW) override {
    NVM_DBG(this, "delegating... pool(" << pool << ")");
    posix_->LowerThreadPoolIOPriority(pool);
  }

  // Sleep/delay the thread for the perscribed number of micro-seconds.
  virtual void SleepForMicroseconds(int micros) override {
    NVM_DBG(this, "delegating... micros(" << micros << ")");
    posix_->SleepForMicroseconds(micros);
  }

  // Returns the status of all threads that belong to the current Env.
  virtual Status GetThreadList(
    std::vector<ThreadStatus>* thread_list
  ) override {
    NVM_DBG(this, "delegating...");
    return posix_->GetThreadList(thread_list);
  }

  // Returns the pointer to ThreadStatusUpdater.  This function will be
  // used in RocksDB internally to update thread status and supports
  // GetThreadList().
  virtual ThreadStatusUpdater* GetThreadStatusUpdater() const override {
    NVM_DBG(this, "delegating...");
    return posix_->GetThreadStatusUpdater();
  }

  // Returns the ID of the current thread.
  virtual uint64_t GetThreadID(void) const override {
    NVM_DBG(this, "delegating...");
    return posix_->GetThreadID();
  }

  // Generates a unique id that can be used to identify a db
  virtual std::string GenerateUniqueId(void) override {
    NVM_DBG(this, "delegating...");
    std::string uid = posix_->GenerateUniqueId();
    NVM_DBG(this, "uid(" << uid << ")");
    return uid;
  }

  Env* posix_;
  NvmStore* store_;

private:

  // No copying allowed
  EnvNVM(const Env&);
  void operator=(const Env&);

  std::string uri_;

  std::map<std::string, std::vector<NvmFile*>> fs_;
  port::Mutex fs_mutex_; // Serializing lookup, creation, and deletion

};

//
// NOTE: Single-threaded access is assumed
//
class NvmSequentialFile : public SequentialFile {
public:
  NvmSequentialFile(void) : SequentialFile() {
    NVM_DBG(file_, "");
  }

  NvmSequentialFile(
    NvmFile *file, const EnvOptions& options
  ) : SequentialFile(), file_(file), pos_() {
    NVM_DBG(file_, "");

    file_->Ref();
  }

  ~NvmSequentialFile(void) {
    NVM_DBG(file_, "");

    file_->Unref();
  }

  virtual Status Read(size_t n, Slice* result, char* scratch) override {

    NVM_DBG(file_, "pos_(" << pos_ << ")");

    if (pos_ >= file_->GetFileSize()) {
      NVM_DBG(file_, "EOF");
      *result = Slice();
      return Status::OK();
    }
    NVM_DBG(file_, "forwarding (fill buffers + read buffers)");

    Status s;

    NVM_DBG(file_, "forwarding");
    s = file_->Read(pos_, n, result, scratch);
    if (!s.ok()) {
      NVM_DBG(file_, "FAILED: forwarded read")
      return s;
    }

    pos_ += result->size();

    NVM_DBG(file_, "pos_(" << pos_ << ")");
    return s;
  }

  virtual Status Skip(uint64_t n) override {
    NVM_DBG(file_, "n(" << n << ")");

    if (n + pos_ > file_->GetFileSize()) {      // TODO: Verify this boundary
      return Status::IOError("Skipping beyond end of file");
    }

    pos_ += n;

    return Status::OK();
  }

  virtual Status InvalidateCache(size_t offset, size_t length) override {
    NVM_DBG(file_, "forwarding");

    return file_->InvalidateCache(offset, length);
  }

protected:
  NvmFile *file_;
  uint64_t pos_;
};

//
// NOTE: Multi-threaded access is assumed
//
class NvmRandomAccessFile : public RandomAccessFile {
public:
  NvmRandomAccessFile(void) : RandomAccessFile() {
    NVM_DBG(file_, "");
  }

  NvmRandomAccessFile(
    NvmFile *file, const EnvOptions& options
  ) : RandomAccessFile(), file_(file) {
    NVM_DBG(file_, "");

    file_->Ref();
  }

  ~NvmRandomAccessFile(void) {
    NVM_DBG(file_, "");

    file_->Unref();
  }

  virtual Status Read(
    uint64_t offset,
    size_t n,
    Slice* result,
    char* scratch
  ) const override {
    NVM_DBG(file_, "forwarding");

    MutexLock lock(&file_->read_mutex_);

    Status s;

    s = file_->Read(offset, n, result, scratch);
    if (!s.ok()) {
      NVM_DBG(file_, "FAILED: failed reading buffers");
    }

    return s;
  }

  virtual bool ShouldForwardRawRequest(void) const override {
    NVM_DBG(file_, "false");

    return false;
  }

  virtual void EnableReadAhead(void) override {
    NVM_DBG(file_, "ignoring");
  }

  // Tries to get an unique ID for this file that will be the same each time
  // the file is opened (and will stay the same while the file is open).
  // Furthermore, it tries to make this ID at most "max_size" bytes. If such an
  // ID can be created this function returns the length of the ID and places it
  // in "id"; otherwise, this function returns 0, in which case "id"
  // may not have been modified.
  //
  // This function guarantees, for IDs from a given environment, two unique ids
  // cannot be made equal to eachother by adding arbitrary bytes to one of
  // them. That is, no unique ID is the prefix of another.
  //
  // This function guarantees that the returned ID will not be interpretable as
  // a single varint.
  //
  // Note: these IDs are only valid for the duration of the process.
  virtual size_t GetUniqueId(char* id, size_t max_size) const override {
    NVM_DBG(file_, "forwarding");

    return file_->GetUniqueId(id, max_size);
  }

  virtual void Hint(AccessPattern pattern) override {
    NVM_DBG(file_, "ignoring");
  }

  virtual Status InvalidateCache(size_t offset, size_t length) override {
    NVM_DBG(file_, "forwarding");

    return file_->InvalidateCache(offset, length);
  }

protected:
  NvmFile *file_;
};

//
// NOTE: Single-threaded access is assumed for access to instances of this
// class.
//
class NvmWritableFile : public WritableFile {
public:
  NvmWritableFile(void) : WritableFile(), truncated_(false) {
    NVM_DBG(this, "truncated_: " << std::boolalpha << truncated_);
  }

  NvmWritableFile(
    NvmFile *file, const EnvOptions& options
  ) : WritableFile(), file_(file), truncated_(false) {
    NVM_DBG(this, "truncated_: " << std::boolalpha << truncated_);

    file_->Ref();
  }

  ~NvmWritableFile(void) {
    NVM_DBG(this, "truncated_: " << std::boolalpha << truncated_);

    file_->Unref();
  }

  virtual size_t GetRequiredBufferAlignment(void) const override {
    NVM_DBG(this, "forwarding");

    return file_->GetRequiredBufferAlignment();
  }

  virtual Status Append(const Slice& slice) override {
    NVM_DBG(this, "forwarding...");

    return file_->Append(slice);
  }

  virtual Status PositionedAppend(const Slice& slice, uint64_t offset) {
    NVM_DBG(this, "NOT SUPPORTED");

    return Status::NotSupported();
  }

  // Truncate is necessary to trim the file to the correct size before closing.
  // It is not always possible to keep track of the file size due to whole pages
  // writes. The behavior is undefined if called with other writes to follow.
  virtual Status Truncate(uint64_t size) override {
    NVM_DBG(this, "forwarding");

    if (!truncated_) {
      truncated_ = true;
      return file_->Truncate(size);
    }

    return Status::IOError("File is already truncated!");
  }

  virtual Status Close(void) override {
    NVM_DBG(this, "truncated_:" << truncated_);

    if (!truncated_) {
      NVM_DBG(this, "Truncating...");
      Truncate(file_->GetFileSize());
    }

    NVM_DBG(this, "Closing...");
    return file_->Close();
  }

  virtual Status Flush(void) override {
    NVM_DBG(this, "forwarding");

    return file_->Flush();
  }

  // sync data
  virtual Status Sync(void) override {
    NVM_DBG(this, "forwarding");

    return file_->Sync();
  }

  //
  // Sync data and/or metadata as well.
  // By default, sync only data.
  // Override this method for environments where we need to sync
  // metadata as well.
  //
  virtual Status Fsync(void) override {
    NVM_DBG(this, "forwarding");

    return file_->Fsync();
  }

  // true if Sync() and Fsync() are safe to call concurrently with Append()
  // and Flush().
  virtual bool IsSyncThreadSafe(void) const override {
    NVM_DBG(this, "forwarding");

    return file_->IsSyncThreadSafe();
  }

  //
  // Change the priority in rate limiter if rate limiting is enabled.
  // If rate limiting is not enabled, this call has no effect.
  //
  virtual void SetIOPriority(Env::IOPriority pri) override {
    NVM_DBG(this, "forwarding");

    io_priority_ = pri;
  }

  virtual Env::IOPriority GetIOPriority(void) override {
    NVM_DBG(this, "caught");

    return io_priority_;
  }

  //
  // Get the size of valid data in the file.
  //
  virtual uint64_t GetFileSize() override {
    NVM_DBG(this, "forwarding");

    return file_->GetFileSize();
  }

  // For documentation, refer to RandomAccessFile::GetUniqueId()
  virtual size_t GetUniqueId(char* id, size_t max_size) const override {
    NVM_DBG(this, "forwarding");

    return file_->GetUniqueId(id, max_size);
  }

  // Remove any kind of caching of data from the offset to offset+length
  // of this file. If the length is 0, then it refers to the end of file.
  // If the system is not caching the file contents, then this is a noop.
  // This call has no effect on dirty pages in the cache.
  virtual Status InvalidateCache(size_t offset, size_t length) override {
    NVM_DBG(this, "forwarding");

    return file_->InvalidateCache(offset, length);
  }

  // Sync a file range with disk.
  // offset is the starting byte of the file range to be synchronized.
  // nbytes specifies the length of the range to be synchronized.
  // This asks the OS to initiate flushing the cached data to disk,
  // without waiting for completion.
  // Default implementation does nothing.
  virtual Status RangeSync(uint64_t offset, uint64_t nbytes) override {
    NVM_DBG(this, "forwarding");

    return file_->RangeSync(offset, nbytes);;
  }

  // PrepareWrite performs any necessary preparation for a write
  // before the write actually occurs.  This allows for pre-allocation
  // of space on devices where it can result in less file
  // fragmentation and/or less waste from over-zealous filesystem
  // pre-allocation.
  virtual void PrepareWrite(size_t offset, size_t len) override {
    /*NVM_DBG(this, "forwarding");*/

    file_->PrepareWrite(offset, len);
  }

  std::string txt(void) const {
    std::stringstream ss;
    ss << "fname(" << file_->info_.fname() << ")";
    return ss.str();
  }

protected:
  //
  // Pre-allocate space for a file.
  //
  virtual Status Allocate(uint64_t offset, uint64_t len) override {
    NVM_DBG(this, "forwarding");

    return file_->Allocate(offset, len);
  }

 private:
  // No copying allowed
  NvmWritableFile(const WritableFile&);
  void operator=(const WritableFile&);

protected:
  NvmFile *file_;

  bool truncated_;
};

}  // namespace rocksdb

#endif  // STORAGE_ROCKSDB_ENVNVM_H_

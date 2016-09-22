#include <iostream>
#include <fstream>
#include <string>
#include <chrono>
#include <linux/limits.h>
#include "rocksdb/utilities/env_registry.h"
#include "env_nvm.h"

namespace rocksdb {

static EnvRegistrar nvm_reg(
  "nvm://",
  [](const std::string& uri, std::unique_ptr<Env>* env_guard) {

    env_guard->reset(new EnvNVM(uri));

    return env_guard->get();
  }
);

std::pair<std::string, std::string> SplitPath(const std::string& path) {

  char sep = '/';
  int sep_idx = path.find_last_of(sep, path.length());
  int fname_idx = sep_idx + 1;

  while(sep_idx > 0 && path[sep_idx] == sep) {
    --sep_idx;
  }

  return std::pair<std::string, std::string>(
      path.substr(0, sep_idx + 1),
      path.substr(fname_idx)
  );
}

// Thread management taken from POSIX ENV -- BEGIN
struct StartThreadState {
  void (*user_function)(void*);
  void* arg;
};

static void* StartThreadWrapper(void* arg) {
  StartThreadState* state = reinterpret_cast<StartThreadState*>(arg);
  state->user_function(state->arg);
  delete state;
  return nullptr;
}

ThreadStatusUpdater* CreateThreadStatusUpdater() {
  return new ThreadStatusUpdater();
}
// Thread management taken from POSIX ENV -- END

EnvNVM::EnvNVM(
  const std::string& uri
) : Env(), uri_(uri), fs_(), thread_pools_(Priority::TOTAL) {
  NVM_TRACE(this, "");

  // Thread management taken from POSIX ENV -- BEGIN
  ThreadPool::PthreadCall("mutex_init", pthread_mutex_init(&mu_, nullptr));
  for (int pool_id = 0; pool_id < Env::Priority::TOTAL; ++pool_id) {
    thread_pools_[pool_id].SetThreadPriority(
        static_cast<Env::Priority>(pool_id)
    );
    thread_pools_[pool_id].SetHostEnv(this);
  }
  thread_status_updater_ = CreateThreadStatusUpdater();
  // Thread management taken from POSIX ENV -- END
}

EnvNVM::~EnvNVM(void) {
  NVM_TRACE(this, "");

  MutexLock lock(&fs_mutex_);

  for (auto dir : fs_) {
    std::cout << dir.first << std::endl;
    for (auto file : dir.second) {
      std::cout << file->GetFname() << "," << file->GetFileSize() << std::endl;
    }
  }

  // Thread management taken from POSIX ENV -- BEGIN
  for (const auto tid : threads_to_join_) {
    pthread_join(tid, nullptr);
  }
  for (int pool_id = 0; pool_id < Env::Priority::TOTAL; ++pool_id) {
    thread_pools_[pool_id].JoinAllThreads();
  }
  // Delete the thread_status_updater_ only when the current Env is not
  // Env::Default().  This is to avoid the free-after-use error when
  // Env::Default() is destructed while some other child threads are
  // still trying to update thread status.
  if (this != Env::Default()) {
    delete thread_status_updater_;
  }
  // Thread management taken from POSIX ENV -- END
}

Status EnvNVM::NewDirectory(
  const std::string& dpath,
  unique_ptr<Directory>* result
) {
  NVM_TRACE(this, "dpath(" << dpath << ")");

  result->reset(new NVMDirectory());

  return Status::OK();
}

Status EnvNVM::NewSequentialFile(
  const std::string& fpath,
  unique_ptr<SequentialFile>* result,
  const EnvOptions& options
) {
  NVM_TRACE(this, "fpath(" << fpath << ")");

  MutexLock lock(&fs_mutex_);

  NVMFile *file = FindFileUnguarded(fpath);
  if (!file) {
    return Status::NotFound();
  }

  result->reset(new NVMSequentialFile(file, options));

  return Status::OK();
}

Status EnvNVM::NewRandomAccessFile(
  const std::string& fpath,
  unique_ptr<RandomAccessFile>* result,
  const EnvOptions& options
) {
  NVM_TRACE(this, "fpath(" << fpath << ")");

  MutexLock lock(&fs_mutex_);

  NVMFile *file = FindFileUnguarded(fpath);
  if (!file) {
    return Status::NotFound();
  }

  result->reset(new NVMRandomAccessFile(file, options));

  return Status::OK();
}

Status EnvNVM::ReuseWritableFile(
  const std::string& fpath,
  const std::string& fpath_old,
  unique_ptr<WritableFile>* result,
  const EnvOptions& options
) {
  NVM_TRACE(this, "fpath(" << fpath << "), fpath_old(" << fpath_old << ")");

  MutexLock lock(&fs_mutex_);

  return Status::IOError("ReuseWritableFile --> Not implemented.");
}

Status EnvNVM::NewWritableFile(
  const std::string& fpath,
  unique_ptr<WritableFile>* result,
  const EnvOptions& options
) {
  NVM_TRACE(this, "fpath(" << fpath << ")");

  MutexLock lock(&fs_mutex_);

  NVMFile *file = FindFileUnguarded(fpath);
  if (file) {
    DeleteFileUnguarded(fpath);
  }

  std::pair<std::string, std::string> fparts = SplitPath(fpath);

  file = new NVMFile(this, fparts.first, fparts.second);
  fs_[file->GetDpath()].push_back(file);

  result->reset(new NVMWritableFile(file, options));

  return Status::OK();
}

//
// Deletes a file without taking the fs_mutex_
//
Status EnvNVM::DeleteFileUnguarded(
  const std::string& dpath, const std::string& fname
) {
  NVM_TRACE(this, "dpath(" << dpath << "), fname(" << fname << ")");

  auto dit = fs_.find(dpath);
  if (dit == fs_.end()) {
    NVM_TRACE(this, "Dir NOT found");
    return Status::NotFound();
  }

  for (auto it = dit->second.begin(); it != dit->second.end(); ++it) {
    if ((*it)->IsNamed(fname)) {
      NVM_TRACE(this, "File found -- erasing");

      NVMFile *file = *it;

      dit->second.erase(it);
      file->Unref();

      return Status::OK();
    }
  }

  NVM_TRACE(this, "File NOT found");
  return Status::NotFound();
}

//
// Deletes a file without taking the fs_mutex_
//
Status EnvNVM::DeleteFileUnguarded(const std::string& fpath) {
  NVM_TRACE(this, "fpath(" << fpath << ")");

  std::pair<std::string, std::string> parts = SplitPath(fpath);

  return DeleteFileUnguarded(parts.first, parts.second);
}

Status EnvNVM::DeleteFile(const std::string& fpath) {
  NVM_TRACE(this, "fpath(" << fpath << ")");
  MutexLock lock(&fs_mutex_);

  std::pair<std::string, std::string> parts = SplitPath(fpath);

  return DeleteFileUnguarded(parts.first, parts.second);
}

Status EnvNVM::FileExists(const std::string& fpath) {
  NVM_TRACE(this, "fpath(" << fpath << ")");
  MutexLock lock(&fs_mutex_);

  if (FindFileUnguarded(fpath)) {
    return Status::OK();
  }

  return Status::NotFound();
}

Status EnvNVM::GetChildren(
  const std::string& dpath,
  std::vector<std::string>* result
) {
  NVM_TRACE(this, "dpath(" << dpath << ")");
  MutexLock lock(&fs_mutex_);

  result->clear();

  auto dir = fs_.find(dpath);

  if (dir == fs_.end()) {
    return Status::IOError("No such dir.");
  }

  for (auto it = dir->second.begin(); it != dir->second.end(); ++it) {
    result->push_back((*it)->GetFname());
    NVM_TRACE(this, "res(" << result->back() << ")");
  }

  return Status::OK();
}

Status EnvNVM::GetChildrenFileAttributes(
  const std::string& dpath,
  std::vector<FileAttributes>* result
) {
  NVM_TRACE(this, "dpath(" << dpath << ")");

  return Status::IOError("GetChildrenFileAttributes --> Not implemented");
}

NVMFile* EnvNVM::FindFileUnguarded(const std::string& fpath) {
  NVM_TRACE(this, "fpath(" << fpath << ")");

  std::pair<std::string, std::string> parts = SplitPath(fpath);

  auto dit = fs_.find(parts.first);
  if (dit == fs_.end()) {
    NVM_TRACE(this, "!found");
    return NULL;
  }

  for (auto it = dit->second.begin(); it != dit->second.end(); ++it) {
    if ((*it)->IsNamed(parts.second)) {
      NVM_TRACE(this, "found");
      return *it;
    }
  }

  NVM_TRACE(this, "!found");
  return NULL;
}

Status EnvNVM::CreateDirIfMissing(const std::string& dpath) {
  NVM_TRACE(this, "dpath(" << dpath << ")");
  MutexLock lock(&fs_mutex_);

  if (fs_.find(dpath) != fs_.end()) {
    return Status::OK();
  }

  fs_.insert(std::pair<std::string, std::vector<NVMFile*> >(
    dpath, std::vector<NVMFile*>()
  ));

  return Status::OK();
}

Status EnvNVM::CreateDir(const std::string& dpath) {
  NVM_TRACE(this, "dpath(" << dpath << ")");
  MutexLock lock(&fs_mutex_);

  if (fs_.find(dpath) != fs_.end()) {
    return Status::IOError("Directory exists");
  }

  fs_.insert(std::pair<std::string, std::vector<NVMFile*> >(
    dpath, std::vector<NVMFile*>()
  ));

  return Status::OK();
}

Status EnvNVM::DeleteDir(const std::string& dpath) {
  NVM_TRACE(this, "dpath(" << dpath << ")");
  MutexLock lock(&fs_mutex_);

  auto dir = fs_.find(dpath);

  if (dir == fs_.end()) {
    return Status::NotFound("Trying to delete a non-existing dpath");
  }
  if (!dir->second.empty()) {
    return Status::IOError("Trying to delete a non-empty dpath");
  }

  fs_.erase(dir);

  return Status::OK();
}

Status EnvNVM::GetFileSize(const std::string& fpath, uint64_t* fsize) {
  NVM_TRACE(this, "fpath(" << fpath << ")");
  MutexLock lock(&fs_mutex_);

  NVMFile *file = FindFileUnguarded(fpath);
  if (!file) {
    return Status::IOError("File not not found");
  }

  *fsize = file->GetFileSize();

  return Status::OK();
}

Status EnvNVM::GetFileModificationTime(
  const std::string& fpath,
  uint64_t* file_mtime
) {
  NVM_TRACE(this, "fpath(" << fpath << ")");
  MutexLock lock(&fs_mutex_);

  return Status::IOError("GetFileModificationTime --> Not implemented");
}

Status EnvNVM::RenameFile(
  const std::string& fpath_src,
  const std::string& fpath_tgt
) {
  NVM_TRACE(this, "fpath_src(" << fpath_src << "), fpath_tgt(" << fpath_tgt << ")");

  auto parts_src = SplitPath(fpath_src);
  auto parts_tgt = SplitPath(fpath_tgt);

  if (parts_src.first.compare(parts_tgt.first)) {
    return Status::IOError("Directory change not supported when renaming");
  }

  MutexLock lock(&fs_mutex_);

  NVMFile *file = FindFileUnguarded(fpath_src);         // Get the source file
  if (!file) {
    return Status::NotFound();
  }

  NVMFile *file_target = FindFileUnguarded(fpath_tgt);  // Delete target
  if (file_target) {
    DeleteFileUnguarded(fpath_tgt);
  }

  file->Rename(parts_tgt.second);                       // The actual renaming

  return Status::OK();
}

Status EnvNVM::LockFile(const std::string& fpath, FileLock** lock) {
  NVM_TRACE(this, "fpath(" << fpath << ")");

  *lock = new FileLock;

  NVM_TRACE(this, "created lock(" << *lock << ")");

  return Status::OK();
}

Status EnvNVM::UnlockFile(FileLock* lock) {
  NVM_TRACE(this, "lock(" << lock << ")");

  delete lock;

  return Status::OK();
}

void EnvNVM::Schedule(
  void (*function)(void* arg),
  void* arg,
  Priority pri,
  void* tag,
  void (*unschedFunction)(void* arg)
) {
  NVM_TRACE(this, "");

  assert(pri >= Priority::LOW && pri <= Priority::HIGH);
  thread_pools_[pri].Schedule(function, arg, tag, unschedFunction);
}

int EnvNVM::UnSchedule(void* arg, Priority pri) {
  NVM_TRACE(this, "");

  return thread_pools_[pri].UnSchedule(arg);
}

void EnvNVM::StartThread(void (*function)(void *), void *arg ) {
  NVM_TRACE(this, "");

  pthread_t t;
  StartThreadState* state = new StartThreadState;
  state->user_function = function;
  state->arg = arg;
  ThreadPool::PthreadCall(
      "start thread", pthread_create(&t, nullptr, &StartThreadWrapper, state));
  ThreadPool::PthreadCall("lock", pthread_mutex_lock(&mu_));
  threads_to_join_.push_back(t);
  ThreadPool::PthreadCall("unlock", pthread_mutex_unlock(&mu_));
}

void EnvNVM::WaitForJoin(void) {
  NVM_TRACE(this, "");

  for (const auto tid : threads_to_join_) {
    pthread_join(tid, nullptr);
  }
  threads_to_join_.clear();
}

unsigned int EnvNVM::GetThreadPoolQueueLen(Priority pri) const {
  NVM_TRACE(this, "");

  assert(pri >= Priority::LOW && pri <= Priority::HIGH);
  return thread_pools_[pri].GetQueueLen();
}

uint64_t EnvNVM::GetThreadID(void) const {
  NVM_TRACE(this, "ignoring...");

  return 0;
}

void EnvNVM::LowerThreadPoolIOPriority(Priority pool) {
  NVM_TRACE(this, "ignoring...");
}

void EnvNVM::SetBackgroundThreads(int num, Priority pri) {
  NVM_TRACE(this, "ignoring...");

  return;
}

void EnvNVM::IncBackgroundThreadsIfNeeded(int num, Priority pri) {
  NVM_TRACE(this, "ignoring...");

  return;
}

uint64_t EnvNVM::NowNanos(void) {
  NVM_TRACE(this, "");

  return NowMicros() * 1000;
};

std::string EnvNVM::GenerateUniqueId(void) {
  NVM_TRACE(this, "");

  std::ifstream uuid_file("/proc/sys/kernel/random/uuid");
  std::string line;

  if (uuid_file.is_open()) {
    getline(uuid_file, line);
    uuid_file.close();
    NVM_TRACE(this, "generated(" << line << ")");
    return line;
  }

  return "foo-bar-not-unique";
};

Status EnvNVM::GetTestDirectory(std::string* path) {
  NVM_TRACE(this, "");

  return Status::NotFound();
}

Status EnvNVM::NewLogger(
  const std::string& fpath,
  shared_ptr<Logger>* result
) {
  NVM_TRACE(this, "fpath(" << fpath << ")");

  NVMLogger *logger = new NVMLogger(InfoLogLevel::INFO_LEVEL);

  result->reset(logger);

  return Status::OK();
}

uint64_t EnvNVM::NowMicros(void) {
  NVM_TRACE(this, "");

  auto now = std::chrono::high_resolution_clock::now();
  auto duration = now.time_since_epoch();
  auto micros = std::chrono::duration_cast<std::chrono::microseconds>(duration);

  return micros.count();
}

void EnvNVM::SleepForMicroseconds(int micros) {
  NVM_TRACE(this, "micros(" << micros << ")");

  return;
}

Status EnvNVM::GetHostName(char* name, uint64_t len) {
  NVM_TRACE(this, "");

  return Status::IOError("GetHostname --> Not implemented");
}

Status EnvNVM::GetCurrentTime(int64_t* unix_time) {
  NVM_TRACE(this, "");

  *unix_time = time(0);

  return Status::OK();
}

std::string EnvNVM::TimeToString(uint64_t stamp) {
  NVM_TRACE(this, "stamp(" << stamp << ")");

  const int BUF_LEN = 100;
  char buf [BUF_LEN];

  time_t raw = stamp;
  struct tm *inf = localtime(&raw);
  strftime(buf, BUF_LEN, "%Y-%m-%d %H:%M:%S", inf);

  return std::string(buf);
}

Status EnvNVM::GetAbsolutePath(
  const std::string& db_path,
  std::string* output_path
) {
  NVM_TRACE(this, "db_path(" << db_path << ")");

  char buf[PATH_MAX];

  if (realpath(db_path.c_str(), buf)) {
    output_path->assign(buf);
    return Status::OK();
  }

  return Status::IOError("Failed retrieving absolute path");
}

}       // namespace rocksdb

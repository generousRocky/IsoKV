#include <iostream>
#include <string>
#include <chrono>
#include <linux/limits.h>
#include <iostream>
#include <fstream>
#include "rocksdb/utilities/env_registry.h"
#include "env_nvm.h"

namespace rocksdb {

static EnvRegistrar nvm_reg(
  "nvm://",
  [](const std::string& uri, std::unique_ptr<Env>* env_guard) {

    NVM_DEBUG("uri(%s)\n", uri.c_str());

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

EnvNVM::EnvNVM(const std::string& device)
  : Env(), device_(device), fs_(), thread_pools_(Priority::TOTAL) {

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

  for (auto dit = fs_.begin(); dit != fs_.end(); ++dit) {
    NVM_DEBUG("dname(%s)\n", dit->first.c_str());
    for (auto fit = dit->second.begin(); fit != dit->second.end(); ++fit) {
      NVM_DEBUG("fname(%s)\n", (*fit)->GetName().c_str());
    }
  }
}


Status EnvNVM::NewDirectory(
  const std::string& name,
  unique_ptr<Directory>* result
) {
  NVM_DEBUG("name(%s), result(?)\n", name.c_str());

  result->reset(new NVMDirectory());

  return Status::OK();
}

Status EnvNVM::NewSequentialFile(
  const std::string& fpath,
  unique_ptr<SequentialFile>* result,
  const EnvOptions& options
) {
  NVM_DEBUG("fpath(%s), result(?), options(?)\n", fpath.c_str());

  NVMFile *file = FindFile(fpath);
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
  NVM_DEBUG("fpath(%s), result(?), options(?)\n", fpath.c_str());

  NVMFile *file = FindFile(fpath);
  if (!file) {
    return Status::NotFound();
  }

  result->reset(new NVMRandomAccessFile(file, options));

  return Status::OK();
}

Status EnvNVM::ReuseWritableFile(
  const std::string& fname,
  const std::string& old_fname,
  unique_ptr<WritableFile>* result,
  const EnvOptions& options
) {
  NVM_DEBUG("fname(%s), old_fname(%s), result(?), options(?)\n",
            fname.c_str(), old_fname.c_str());

  return Status::IOError("ReuseWritableFile --> Not implemented.");
}

Status EnvNVM::NewWritableFile(
  const std::string& fpath,
  unique_ptr<WritableFile>* result,
  const EnvOptions& options
) {
  NVM_DEBUG("fpath(%s), result(?), options(?)\n", fpath.c_str());

  NVMFile *file = FindFile(fpath);
  if (file) {
    DeleteFile(fpath);
  }

  std::pair<std::string, std::string> fparts = SplitPath(fpath);
  NVM_DEBUG("first(%s), second(%s)\n",
            fparts.first.c_str(), fparts.second.c_str());

  file = new NVMFile(this, fparts.first, fparts.second);
  fs_[file->GetDir()].push_back(file);

  result->reset(new NVMWritableFile(file, options));

  return Status::OK();
}

Status EnvNVM::DeleteFile(const std::string& fpath) {
  NVM_DEBUG("fpath(%s)\n", fpath.c_str());

  std::pair<std::string, std::string> parts = SplitPath(fpath);

  auto dit = fs_.find(parts.first);
  if (dit == fs_.end()) {
    NVM_DEBUG("Dir NOT found\n");
    return Status::NotFound();
  }

  for (auto it = dit->second.begin(); it != dit->second.end(); ++it) {
    if ((*it)->IsNamed(parts.second)) {
      NVM_DEBUG("File found -- erasing\n");

      NVMFile *file = *it;

      dit->second.erase(it);
      file->Unref();

      return Status::OK();
    }
  }

  NVM_DEBUG("File NOT found\n");
  return Status::NotFound();
}

Status EnvNVM::FileExists(const std::string& fpath) {
  NVM_DEBUG("fpath(%s)\n", fpath.c_str());

  if (FindFile(fpath)) {
    return Status::OK();
  }

  return Status::NotFound();
}

Status EnvNVM::GetChildren(
  const std::string& dname,
  std::vector<std::string>* result
) {
  NVM_DEBUG("dir(%s), result(%p)\n", dname.c_str(), result);

  result->clear();

  auto dir = fs_.find(dname);

  if (dir == fs_.end()) {
    return Status::IOError("No such dir.");
  }

  for (auto it = dir->second.begin(); it != dir->second.end(); ++it) {
    result->push_back((*it)->GetName());
    NVM_DEBUG("res(%s)\n", result->back().c_str());
  }

  return Status::OK();
}

Status EnvNVM::GetChildrenFileAttributes(
  const std::string& dir,
  std::vector<FileAttributes>* result
) {
  NVM_DEBUG("dir(%s), result(?)\n", dir.c_str());

  return Status::IOError("GetChildrenFileAttributes --> Not implemented");
}

NVMFile* EnvNVM::FindFile(const std::string& fpath) {
  NVM_DEBUG("fpath(%s)\n", fpath.c_str());

  std::pair<std::string, std::string> parts = SplitPath(fpath);

  auto dit = fs_.find(parts.first);
  if (dit == fs_.end()) {
    NVM_DEBUG("!found\n");
    return NULL;
  }

  for (auto it = dit->second.begin(); it != dit->second.end(); ++it) {
    if ((*it)->IsNamed(parts.second)) {
      NVM_DEBUG("found\n");
      return *it;
    }
  }

  NVM_DEBUG("!found\n");
  return NULL;
}

Status EnvNVM::CreateDirIfMissing(const std::string& name) {
  NVM_DEBUG("name(%s)\n", name.c_str());

  fs_.insert(std::pair<std::string, std::vector<NVMFile*> >(
    name, std::vector<NVMFile*>()
  ));

  return Status::OK();
}

Status EnvNVM::CreateDir(const std::string& dpath) {
  NVM_DEBUG("dpath(%s)\n", dpath.c_str());

  if (fs_.find(dpath) != fs_.end()) {
    return Status::IOError("Directory exists");
  }

  return CreateDirIfMissing(dpath);
}

Status EnvNVM::DeleteDir(const std::string& dpath) {
  NVM_DEBUG("dpath(%s)\n", dpath.c_str());

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
  NVM_DEBUG("fpath(%s)\n", fpath.c_str());

  NVMFile *file = FindFile(fpath);
  if (!file) {
    return Status::IOError("File not not found");
  }

  *fsize = file->GetFileSize();

  return Status::OK();
}

Status EnvNVM::GetFileModificationTime(
  const std::string& fname,
  uint64_t* file_mtime
) {
  NVM_DEBUG("\n");

  return Status::IOError("GetFileModificationTime --> Not implemented");
}

Status EnvNVM::RenameFile(
  const std::string& fpath_s,
  const std::string& fpath_t
) {
  NVM_DEBUG("fpath_s(%s), fpath_t(%s)\n", fpath_s.c_str(), fpath_t.c_str());

  NVMFile *file = FindFile(fpath_s);            // Find file to rename
  if (!file) {
    NVM_DEBUG("Cannot find file to rename\n");
    return Status::NotFound();
  }

  NVMFile *file_target = FindFile(fpath_t);     // Delete possibly existing file
  if (file_target) {
    NVM_DEBUG("Target exists.. deleting it...\n");
    DeleteFile(fpath_t);
  }

  auto parts = SplitPath(fpath_t);

  if (file->GetDir().compare(parts.first)) {
    NVM_DEBUG("Changing directory is not supported\n");
    return Status::IOError("Directory change not supported when renaming");
  }

  file->Rename(parts.second);                   // Do the actual renaming

  return Status::OK();
}

Status EnvNVM::LockFile(const std::string& fname, FileLock** lock) {
  NVM_DEBUG("fname(%s), lock(%p)\n", fname.c_str(), *lock);

  *lock = new FileLock;

  NVM_DEBUG("created lock(%p)\n", *lock);

  return Status::OK();
}

Status EnvNVM::UnlockFile(FileLock* lock) {
  NVM_DEBUG("lock(%p)\n", lock);

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
  NVM_DEBUG("function(%p), arg(%p), pri(%d), tag(%p), unschedFunction(%p)\n",
            function, arg, pri, tag, unschedFunction);

  assert(pri >= Priority::LOW && pri <= Priority::HIGH);
  thread_pools_[pri].Schedule(function, arg, tag, unschedFunction);
}

int EnvNVM::UnSchedule(void* arg, Priority pri) {
  NVM_DEBUG("arg(%p), pri(%d)\n", arg, pri);

  return thread_pools_[pri].UnSchedule(arg);
}

void EnvNVM::StartThread(void (*function)(void *), void *arg ) {
  NVM_DEBUG("function(%p), arg(%p)\n", function, arg);

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
  for (const auto tid : threads_to_join_) {
    pthread_join(tid, nullptr);
  }
  threads_to_join_.clear();
}

unsigned int EnvNVM::GetThreadPoolQueueLen(Priority pri) const {
  NVM_DEBUG("pri(%d)\n", pri);

  assert(pri >= Priority::LOW && pri <= Priority::HIGH);
  return thread_pools_[pri].GetQueueLen();
}

uint64_t EnvNVM::GetThreadID(void) const {
  return 0;
}

void EnvNVM::LowerThreadPoolIOPriority(Priority pool) {
  NVM_DEBUG("pool(%d)\n", pool);
}

uint64_t EnvNVM::NowNanos(void) {
  NVM_DEBUG("\n");

  return 0;
};

std::string EnvNVM::GenerateUniqueId(void) {

  std::ifstream uuid_file("/proc/sys/kernel/random/uuid");
  std::string line;

  if (uuid_file.is_open()) {
    getline(uuid_file, line);
    uuid_file.close();
    NVM_DEBUG("generated(%s)\n", line.c_str());
    return line;
  }

  return "foo-bar-not-unique";
};

Status EnvNVM::GetTestDirectory(std::string* path) {
  NVM_DEBUG("\n");

  return Status::NotFound();
}

Status EnvNVM::NewLogger(
  const std::string& fname,
  shared_ptr<Logger>* result
) {
  NVMLogger *logger = new NVMLogger(InfoLogLevel::INFO_LEVEL);

  NVM_DEBUG(" fname(%s), result(%p)\n", fname.c_str(), logger);

  result->reset(logger);

  return Status::OK();
}

uint64_t EnvNVM::NowMicros(void) {
  NVM_DEBUG("\n");

  auto now = std::chrono::high_resolution_clock::now();
  auto duration = now.time_since_epoch();
  auto micros = std::chrono::duration_cast<std::chrono::microseconds>(duration);

  return micros.count();
}

void EnvNVM::SleepForMicroseconds(int micros) {
  NVM_DEBUG("micros(%d)\n", micros);

  return;
}

Status EnvNVM::GetHostName(char* name, uint64_t len) {
  NVM_DEBUG("name(%s), len(%lu)\n", name, len);

  return Status::IOError("GetHostname --> Not implemented");
}

Status EnvNVM::GetCurrentTime(int64_t* unix_time) {
  NVM_DEBUG("unix_time(%ld)\n", *unix_time);

  *unix_time = time(0);

  return Status::OK();
}

std::string EnvNVM::TimeToString(uint64_t stamp) {
  NVM_DEBUG("stamp(%lu)\n", stamp);

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
  NVM_DEBUG("db_path(%s)\n", db_path.c_str());

  char buf[PATH_MAX];

  if (realpath(db_path.c_str(), buf)) {
    output_path->assign(buf);
    return Status::OK();
  }

  return Status::IOError("Failed retrieving absolute path");
}

void EnvNVM::SetBackgroundThreads(int num, Priority pri) {
  NVM_DEBUG(" num(%d), pri(%d)\n", num, pri);

  return;
}

void EnvNVM::IncBackgroundThreadsIfNeeded(int num, Priority pri) {
  NVM_DEBUG(" num(%d), pri(%d)\n", num, pri);

  return;
}

}       // namespace rocksdb

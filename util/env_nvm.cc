//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifdef ROCKSDB_PLATFORM_NVM

#include "db/filename.h"
#include "nvm/nvm.h"
#include "util/string_util.h"

namespace rocksdb {

struct StartThreadState {
  void (*user_function)(void*);
  void* arg;
};

// list of pathnames that are locked
static std::set<std::string> lockedFiles;
static port::Mutex mutex_lockedFiles;

static void* StartThreadWrapper(void* arg) {
  StartThreadState* state = reinterpret_cast<StartThreadState*>(arg);
  state->user_function(state->arg);

  delete state;
  return nullptr;
}

ThreadStatusUpdater* CreateThreadStatusUpdater() {
  return new ThreadStatusUpdater();
}

static int PosixLockOrUnlock(const std::string& fname, int fd, bool lock) {
  mutex_lockedFiles.Lock();
  if (lock) {
    // If it already exists in the lockedFiles set, then it is already locked,
    // and fail this lock attempt. Otherwise, insert it into lockedFiles.
    // This check is needed because fcntl() does not detect lock conflict
    // if the fcntl is issued by the same thread that earlier acquired
    // this lock.
    if (lockedFiles.insert(fname).second == false) {
      mutex_lockedFiles.Unlock();
      errno = ENOLCK;
      return -1;
    }
  } else {
    // If we are unlocking, then verify that we had locked it earlier,
    // it should already exist in lockedFiles. Remove it from lockedFiles.
    if (lockedFiles.erase(fname) != 1) {
      mutex_lockedFiles.Unlock();
      errno = ENOLCK;
      return -1;
    }
  }
  errno = 0;
  struct flock f;
  memset(&f, 0, sizeof(f));
  f.l_type = (lock ? F_WRLCK : F_UNLCK);
  f.l_whence = SEEK_SET;
  f.l_start = 0;
  f.l_len = 0;        // Lock/unlock entire file
  int value = fcntl(fd, F_SETLK, &f);
  if (value == -1 && lock) {
    // if there is an error in locking, then remove the pathname from lockedfiles
    lockedFiles.erase(fname);
  }
  mutex_lockedFiles.Unlock();
  return value;
}

static int DFlashLockOrUnlock(const std::string& fname, nvm_file *fd, bool lock) {
  mutex_lockedFiles.Lock();
  if (lock) {
    // If it already exists in the lockedFiles set, then it is already locked,
    // and fail this lock attempt. Otherwise, insert it into lockedFiles.
    if (lockedFiles.insert(fname).second == false) {
      mutex_lockedFiles.Unlock();
      errno = ENOLCK;
      return -1;
    }
  } else {
    // If we are unlocking, then verify that we had locked it earlier,
    // it should already exist in lockedFiles. Remove it from lockedFiles.
    if (lockedFiles.erase(fname) != 1) {
      mutex_lockedFiles.Unlock();
      errno = ENOLCK;
      return -1;
    }
  }

  errno = 0;

  if (lock) {
    if (fd->LockFile()) {
      return -1;
    }
  } else {
    fd->UnlockFile();
  }

  mutex_lockedFiles.Unlock();
  return 0;
}

bool PosixFileLock::Unlock() {
  if (PosixLockOrUnlock(filename, fd_, false) == -1) {
    return false;
  }
  close(fd_);
  return true;
}

bool NVMFileLock::Unlock() {
  if (DFlashLockOrUnlock(filename, fd_, false) == -1) {
    return false;
  }

  root_dir_->nvm_fclose(fd_, "l");
  return true;
}

static void PthreadCall(const char* label, int result) {
  if (result != 0) {
    fprintf(stderr, "pthread %s: %s\n", label, strerror(result));
    abort();
  }
}

// Parses the filename and sets file type.
bool ParseType(std::string fname, FileType* type) {
  uint64_t num;
  size_t dir_pos;
  std::string filename;

  // Asume dbname/file as in filename filename.cc
  dir_pos = fname.find_first_of("/") + 1; // Account for "/"
  filename.append(fname.c_str() + dir_pos);
  return ParseFileName(filename, &num, type);
}

class NVMEnv : public Env {
 public:
  NVMEnv() :
    checkedDiskForMmap_(false),
    forceMmapOff(false),
    page_size_(getpagesize()),
    thread_pools_(Priority::TOTAL) {

    PthreadCall("mutex_init", pthread_mutex_init(&mu_, nullptr));
    for (int pool_id = 0; pool_id < Env::Priority::TOTAL; ++pool_id) {
      thread_pools_[pool_id].SetThreadPriority(static_cast<Env::Priority>(pool_id));

      // This allows later initializing the thread-local-env of each thread.
      thread_pools_[pool_id].SetHostEnv(this);
    }
    thread_status_updater_ = CreateThreadStatusUpdater();

    ALLOC_CLASS(nvm_api, nvm());
    ALLOC_CLASS(root_dir, nvm_directory("root", 4, nvm_api, nullptr));

    LoadFtl();
  }

  virtual ~NVMEnv() {
    SaveFTL();

    for (const auto tid : threads_to_join_) {
      pthread_join(tid, nullptr);
    }

    for (int pool_id = 0; pool_id < Env::Priority::TOTAL; ++pool_id) {
      thread_pools_[pool_id].JoinAllThreads();
    }

    delete thread_status_updater_;
    delete root_dir;
    delete nvm_api;
  }

  virtual Status GarbageCollect() override {
    NVM_DEBUG("doing garbage collect");

    nvm_api->GarbageCollection();

    return Status::OK();
  }

  virtual Status SaveFTL() override {
    printf("saving ftl");

    int fd = open(ftl_save_location, O_RDWR | O_CREAT, S_IWUSR | S_IRUSR);

    if (fd < 0) {
      return Status::IOError("Unable to create save ftl file");
    }

    if (!root_dir->Save(fd).ok()) {
      close(fd);
      return Status::IOError("Unable to save directory");
    }

    close(fd);
    return Status::OK();
  }

  void SetFD_CLOEXEC(int fd, const EnvOptions* options) {
    if ((options == nullptr || options->set_fd_cloexec) && fd > 0) {
      fcntl(fd, F_SETFD, fcntl(fd, F_GETFD) | FD_CLOEXEC);
    }
  }

  virtual Status NewSequentialFile(const std::string& fname,
      unique_ptr<SequentialFile>* result, const EnvOptions& options) override {
    result->reset();
    FileType type;
    if (!ParseType(fname, &type)) {
      return Status::IOError("Cannot determine type for filename %s",
                                                                fname.c_str());
    }

    if (type == kInfoLogFile || type == kCurrentFile ||
                              type == kDBLockFile || type == kIdentityFile) {
      FILE* f = nullptr;
      do {
        IOSTATS_TIMER_GUARD(open_nanos);
        f = fopen(fname.c_str(), "r");
      } while (f == nullptr && errno == EINTR);
      if (f == nullptr) {
        *result = nullptr;
        return IOError(fname, errno);
      } else {
        int fd = fileno(f);
        SetFD_CLOEXEC(fd, &options);
        result->reset(new PosixSequentialFile(fname, f, options));
        return Status::OK();
      }
    } else {
      nvm_file *f = root_dir->nvm_fopen(fname.c_str(), "r");

      if (f == nullptr) {
        *result = nullptr;
        NVM_DEBUG("unable to open file for read %s", fname.c_str());
        return Status::IOError("unable to open file for read");
      }

      result->reset(new NVMSequentialFile(fname, f, root_dir));
      return Status::OK();
    }
  }

  virtual Status NewRandomAccessFile(const std::string& fname,
      unique_ptr<RandomAccessFile>* result, const EnvOptions& options) override {
    result->reset();
    FileType type;
    if (!ParseType(fname, &type)) {
      return Status::IOError("Cannot determine type for filename %s",
                                                                fname.c_str());
    }

    if (type == kInfoLogFile || type == kCurrentFile ||
                              type == kDBLockFile || type == kIdentityFile) {
      Status s;
      int fd;
      {
        IOSTATS_TIMER_GUARD(open_nanos);
        fd = open(fname.c_str(), O_RDONLY);
      }
      SetFD_CLOEXEC(fd, &options);
      if (fd < 0) {
        s = IOError(fname, errno);
      } else if (options.use_mmap_reads && sizeof(void*) >= 8) {
        // Use of mmap for random reads has been removed because it
        // kills performance when storage is fast.
        // Use mmap when virtual address-space is plentiful.
        uint64_t size;
        s = GetFileSize(fname, &size);
        if (s.ok()) {
          void* base = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
          if (base != MAP_FAILED) {
            result->reset(new PosixMmapReadableFile(fd, fname, base,
                                                  size, options));
          } else {
            s = IOError(fname, errno);
          }
        }
        close(fd);
      } else {
        result->reset(new PosixRandomAccessFile(fname, fd, options));
      }
      return s;
    } else {
      nvm_file *f = root_dir->nvm_fopen(fname.c_str(), "r");
      if (f == nullptr) {
        *result = nullptr;
        NVM_DEBUG("unable to open file for read %s", fname.c_str());
        return Status::IOError("unable to open file for read");
      }
      result->reset(new NVMRandomAccessFile(fname, f, root_dir));
      return Status::OK();
    }
  }

  virtual Status NewWritableFile(const std::string& fname,
        unique_ptr<WritableFile>* result, const EnvOptions& options) override {
    result->reset();
    FileType type;
    if (!ParseType(fname, &type)) {
      return Status::IOError("Cannot determine type for filename %s",
                                                                fname.c_str());
    }

    // For LOG, CURRENT, LOCK, and IDENTITY use the filesystem partition where
    // RocksDB code is code. Use normal posix for these.
    // TODO: This copy is taken from env_posix.cc. Since it makes sense to
    // combine posix with other storage backends, decouple this part of the code
    // form the posix environment so that PosixXFile classes can be used
    // simultaneously with a different storage backend.
    if (type == kInfoLogFile ||
        type == kCurrentFile ||
        type == kDBLockFile ||
        type == kIdentityFile ||
        options.type == kCurrentFile) {
      Status s;
      int fd = -1;
      do {
        IOSTATS_TIMER_GUARD(open_nanos);
        fd = open(fname.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
      } while (fd < 0 && errno == EINTR);
      if (fd < 0) {
        s = IOError(fname, errno);
      } else {
        // SetFD_CLOEXEC(fd, &options);
        if (options.use_mmap_writes) {
          if (!checkedDiskForMmap_) {
            // this will be executed once in the program's lifetime.
            // do not use mmapWrite on non ext-3/xfs/tmpfs systems.
            if (!SupportsFastAllocate(fname)) {
              forceMmapOff = true;
            }
            checkedDiskForMmap_ = true;
          }
        }
        if (options.use_mmap_writes && !forceMmapOff) {
          result->reset(new PosixMmapFile(fname, fd, page_size_, options));
        } else {
          // disable mmap writes
          EnvOptions no_mmap_writes_options = options;
          no_mmap_writes_options.use_mmap_writes = false;

          result->reset(new PosixWritableFile(fname, fd, no_mmap_writes_options));
        }
      }
    // For data and metadata use DFlash backend interfacing an Open-Channel SSD
    } else {
      nvm_file *fd = root_dir->nvm_fopen(fname.c_str(), "a");

      if (fd == nullptr) {
        *result = nullptr;
        NVM_DEBUG("unable to open file for write %s", fname.c_str());
        return Status::IOError("unable to open file for write");
      }

      NVMWritableFile *writable_file;
      ALLOC_CLASS(writable_file, NVMWritableFile(fname, fd, root_dir));
      fd->SetSeqWritableFile(writable_file);
      result->reset(writable_file);
    }
    return Status::OK();
  }

  virtual Status NewRandomRWFile(const std::string& fname,
        unique_ptr<RandomRWFile>* result, const EnvOptions& options) override {
    result->reset();
    FileType type;
    if (!ParseType(fname, &type)) {
      return Status::IOError("Cannot determine type for filename %s",
                                                                fname.c_str());
    }

    if (type == kInfoLogFile || type == kCurrentFile ||
                              type == kDBLockFile || type == kIdentityFile) {
      // no support for mmap yet
      if (options.use_mmap_writes || options.use_mmap_reads) {
        return Status::NotSupported("No support for mmap read/write yet");
      }
      Status s;
      int fd;
      {
        IOSTATS_TIMER_GUARD(open_nanos);
        fd = open(fname.c_str(), O_CREAT | O_RDWR, 0644);
      }
      if (fd < 0) {
        s = IOError(fname, errno);
      } else {
        SetFD_CLOEXEC(fd, &options);
        result->reset(new PosixRandomRWFile(fname, fd, options));
      }
      return s;
    } else {
      nvm_file *fd = root_dir->nvm_fopen(fname.c_str(), "w");
      if (fd == nullptr) {
        *result = nullptr;
        NVM_DEBUG("unable to open file for write %s", fname.c_str());
        return Status::IOError("unable to open file for write");
      }
      result->reset(new NVMRandomRWFile(fname, fd, root_dir));
      return Status::OK();
    }
  }

  // For now, replicate the directory tree for bot posix and dflash.
  virtual Status NewDirectory(const std::string& name,
                                      unique_ptr<Directory>* result) override {
    result->reset();

    // DFlash
    nvm_directory *fd = root_dir->OpenDirectory(name.c_str());
    if (fd == nullptr) {
      NVM_DEBUG("directory %s not found", name.c_str());
      return Status::IOError("directory doesn't exist");
    }

    result->reset(new NVMDirectory(fd));

    // Posix
    int posix_fd;
    {
      IOSTATS_TIMER_GUARD(open_nanos);
      posix_fd = open(name.c_str(), 0);
    }
    if (posix_fd < 0) {
      return IOError(name, errno);
    } else {
      result->reset(new PosixDirectory(posix_fd));
    }

    return Status::OK();
  }

  virtual Status FileExists(const std::string& fname) override {
    bool exists = root_dir->FileExists(fname.c_str());
    FileType type;
    if (!ParseType(fname, &type)) {
      return Status::IOError("Cannot determine type for filename %s",
                                                                fname.c_str());
    }

    //Posix
    if (type == kInfoLogFile || type == kCurrentFile ||
                              type == kDBLockFile || type == kIdentityFile) {

      int result = access(fname.c_str(), F_OK);
      if (result == 0) {
        return Status::OK();
      }

      switch (errno) {
        case EACCES:
        case ELOOP:
        case ENAMETOOLONG:
        case ENOENT:
        case ENOTDIR:
          return Status::NotFound();
        default:
          assert(result == EIO || result == ENOMEM);
          return Status::IOError("Unexpected error(" + ToString(result) +
                               ") accessing file `" + fname + "' ");
      }
    // Dflash
    } else {
      if (exists) {
        NVM_DEBUG("%s exists: %d", fname.c_str(), exists ? 1 : 0);
        return Status::OK();
      } else {
        return Status::NotFound();
      }
    }
  }

  // We need to return the files in both directories: the one created in posix
  // and the one created in dflash. We explore both and return the union. Note
  // that the files in posix and dflash are mutually exclusive either way.
  virtual Status GetChildren(const std::string& dir,
                                  std::vector<std::string>* result) override {
    result->clear();

    // DFlash
    if (!root_dir->GetChildren(dir.c_str(), result) == 0) {
      return Status::IOError("Could not get children for DFlash\n");
    }

    // Posix
    DIR* d = opendir(dir.c_str());
    if (d == nullptr) {
      return IOError(dir, errno);
    }
    struct dirent* entry;
    while ((entry = readdir(d)) != nullptr) {
      result->push_back(entry->d_name);
    }
    closedir(d);

    return Status::OK();
  }

  virtual Status DeleteFile(const std::string& fname) override {
    FileType type;
    if (!ParseType(fname, &type)) {
      return Status::IOError("Cannot determine type for filename %s",
                                                                fname.c_str());
    }

    if (type == kInfoLogFile || type == kCurrentFile ||
                              type == kDBLockFile || type == kIdentityFile) {
      Status result;
      if (unlink(fname.c_str()) != 0) {
        result = IOError(fname, errno);
      }
      return result;
    } else {
      if (root_dir->DeleteFile(fname.c_str())) {
        return Status::IOError("delete file failed");
      }
    }
    return Status::OK();
  }

  // As in NewDir, replicate the directory tree bot posix and dflas
  virtual Status CreateDir(const std::string& name) override {
    Status result;

    // DFlash
    if (root_dir->CreateDirectory(name.c_str()) == 0) {
      result = Status::OK();
    } else {
      return Status::IOError("createdir failed");
    }

    // Posix
    if (mkdir(name.c_str(), 0755) != 0) {
      if (errno != EEXIST) {
        result = IOError(name, errno);
      } else if (!DirExists(name)) { // Check that name is actually a
                                     // directory.
        // Message is taken from mkdir
        result = Status::IOError("`"+name+"' exists but is not a directory");
      }
    }
    return result;
  }

  virtual Status CreateDirIfMissing(const std::string& name) override {
    return CreateDir(name);
  }

  // Delete in bloth trees
  virtual Status DeleteDir(const std::string& name) override {
    Status result;

    //  DFlash
    if (root_dir->DeleteDirectory(name.c_str()) == 0) {
      result = Status::OK();
    } else {
      return Status::IOError("delete dir failed");
    }

    if (rmdir(name.c_str()) != 0) {
      result = IOError(name, errno);
    }
    return result;
  }

  virtual Status GetFileSize(const std::string& fname, uint64_t* size) override {
    FileType type;
    if (!ParseType(fname, &type)) {
      return Status::IOError("Cannot determine type for filename %s",
                                                                fname.c_str());
    }

    if (type == kInfoLogFile || type == kCurrentFile ||
                              type == kDBLockFile || type == kIdentityFile) {
      Status s;
      struct stat sbuf;
      if (stat(fname.c_str(), &sbuf) != 0) {
        *size = 0;
        s = IOError(fname, errno);
      } else {
        *size = sbuf.st_size;
      }
       return s;
    } else {
      if (root_dir->GetFileSize(fname.c_str(), size)) {
        return Status::IOError("get file size failed");
      }
      return Status::OK();
    }
  }

  virtual Status GetFileModificationTime(const std::string& fname,
                                               uint64_t* file_mtime) override {
    FileType type;
    if (!ParseType(fname, &type)) {
      return Status::IOError("Cannot determine type for filename %s",
                                                                fname.c_str());
    }

    if (type == kInfoLogFile || type == kCurrentFile ||
                              type == kDBLockFile || type == kIdentityFile) {
      struct stat s;
      if (stat(fname.c_str(), &s) !=0) {
        return IOError(fname, errno);
      }
      *file_mtime = static_cast<uint64_t>(s.st_mtime);
      return Status::OK();
    } else {
      if (root_dir->GetFileModificationTime(fname.c_str(), (time_t *)file_mtime)) {
        return Status::IOError("get file modification time failed");
      }
      return Status::OK();
    }
  }

  virtual Status RenameFile(const std::string& src,
                                          const std::string& target) override {
    NVM_DEBUG("renaming %s to %s", src.c_str(), target.c_str());
    FileType type;
    if (!ParseType(target, &type)) {
      return Status::IOError("Cannot determine type for filename %s",
                                                                target.c_str());
    }

    if (type == kInfoLogFile || type == kCurrentFile ||
                              type == kDBLockFile || type == kIdentityFile) {
      Status result;
      if (rename(src.c_str(), target.c_str()) != 0) {
        result = IOError(src, errno);
      }
      return result;
    } else {
      if(root_dir->RenameFile(src.c_str(), target.c_str()) == 0) {
        return Status::OK();
      }

      if(root_dir->RenameDirectory(src.c_str(), target.c_str()) == 0) {
        return Status::OK();
      }

      NVM_DEBUG("Failed to rename %s to %s", src.c_str(), target.c_str());
      return Status::IOError("nvm rename file failed");
    }
  }

  virtual Status LinkFile(const std::string& src,
                                          const std::string& target) override {
    FileType type;
    if (!ParseType(target, &type)) {
      return Status::IOError("Cannot determine type for filename %s",
                                                                target.c_str());
    }

    if (type == kInfoLogFile || type == kCurrentFile ||
                              type == kDBLockFile || type == kIdentityFile) {
      Status result;
      if (link(src.c_str(), target.c_str()) != 0) {
        if (errno == EXDEV) {
          return Status::NotSupported("No cross FS links allowed");
        }
        result = IOError(src, errno);
      }
      return result;
    } else {
      if (root_dir->LinkFile(src.c_str(), target.c_str()) != 0) {
        return Status::IOError("nvm link file failed");
      }
      return Status::OK();
    }
  }

  virtual Status LockFile(const std::string& fname, FileLock** lock) override {
    FileType type;
    if (!ParseType(fname, &type)) {
      return Status::IOError("Cannot determine type for filename %s",
                                                                fname.c_str());
    }

    *lock = nullptr;
    if (type == kInfoLogFile || type == kCurrentFile ||
                              type == kDBLockFile || type == kIdentityFile) {
      Status result;
      int fd;
      {
        IOSTATS_TIMER_GUARD(open_nanos);
        fd = open(fname.c_str(), O_RDWR | O_CREAT, 0644);
      }
      if (fd < 0) {
        result = IOError(fname, errno);
      } else if (PosixLockOrUnlock(fname, fd, true) == -1) {
        result = IOError("lock " + fname, errno);
        close(fd);
      } else {
        SetFD_CLOEXEC(fd, nullptr);
        PosixFileLock* my_lock = new PosixFileLock;
        my_lock->fd_ = fd;
        my_lock->filename = fname;
        *lock = my_lock;
      }
      return result;
    } else {
      nvm_file *f = root_dir->nvm_fopen(fname.c_str(), "l");
      if (DFlashLockOrUnlock(fname, f, true) == -1) {
        root_dir->nvm_fclose(f, "l");
        return Status::IOError("unable to lock file");
      }

      NVMFileLock *my_lock = new NVMFileLock;
      my_lock->fd_ = f;
      my_lock->filename = fname;
      my_lock->root_dir_ = root_dir;
      *lock = my_lock;
    }
    return Status::OK();
  }

  virtual Status UnlockFile(FileLock* lock) override {
    return (lock->Unlock() ? Status::OK() : IOError("unlock", errno));
    delete lock;
  }

  virtual void Schedule(void (*function)(void* arg1), void* arg,
                          Priority pri = LOW, void* tag = nullptr) override {
    assert(pri >= Priority::LOW && pri <= Priority::HIGH);
    thread_pools_[pri].Schedule(function, arg, tag);
  }

  virtual int UnSchedule(void* arg, Priority pri) override {
    return thread_pools_[pri].UnSchedule(arg);
  }

  virtual void StartThread(void (*function)(void* arg), void* arg) override {
    pthread_t t;

    StartThreadState* state = new StartThreadState;
    state->user_function = function;
    state->arg = arg;

    PthreadCall("start thread", pthread_create(&t, nullptr,
                                                &StartThreadWrapper, state));
    PthreadCall("lock", pthread_mutex_lock(&mu_));

    threads_to_join_.push_back(t);

    PthreadCall("unlock", pthread_mutex_unlock(&mu_));
  }

  virtual void WaitForJoin() override {
    for (const auto tid : threads_to_join_) {
      pthread_join(tid, nullptr);
    }
    threads_to_join_.clear();
  }

  virtual unsigned int GetThreadPoolQueueLen(Priority pri = LOW) const override {
    assert(pri >= Priority::LOW && pri <= Priority::HIGH);
    return thread_pools_[pri].GetQueueLen();
  }

  virtual Status GetTestDirectory(std::string* result) override {
    *result = "rocksdb";
    return Status::OK();
  }

  virtual Status GetThreadList(std::vector<ThreadStatus>* thread_list) override {
    assert(thread_status_updater_);
    return thread_status_updater_->GetThreadList(thread_list);
  }

  static uint64_t gettid(pthread_t tid) {
    uint64_t thread_id = 0;
    memcpy(&thread_id, &tid, std::min(sizeof(thread_id), sizeof(tid)));
    return thread_id;
  }

  static uint64_t gettid() {
    pthread_t tid = pthread_self();
    return gettid(tid);
  }

#if 0 //We want to log in a file present in the FS - posix
  virtual Status NewLogger(const std::string& fname,
                                        shared_ptr<Logger>* result) override {
    remove("./LOG");

    FILE* f = fopen("./LOG", "w");
    if (f == nullptr) {
      result->reset();
      return IOError(fname, errno);
    } else {
      result->reset(new NVMLogger(f, &NVMEnv::gettid, this));
      return Status::OK();
    }
  }
#endif

  virtual Status NewLogger(const std::string& fname,
                           shared_ptr<Logger>* result) override {
    FILE* f;
    {
      IOSTATS_TIMER_GUARD(open_nanos);
      f = fopen(fname.c_str(), "w");
    }
    if (f == nullptr) {
      result->reset();
      return IOError(fname, errno);
    } else {
      int fd = fileno(f);
      SetFD_CLOEXEC(fd, nullptr);
      result->reset(new PosixLogger(f, &NVMEnv::gettid, this));
      return Status::OK();
    }
  }

  virtual uint64_t NowMicros() override {
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
  }

  virtual uint64_t NowNanos() override {

#if defined(OS_LINUX) || defined(OS_FREEBSD)
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return static_cast<uint64_t>(ts.tv_sec) * 1000000000 + ts.tv_nsec;
#elif defined(__MACH__)
    clock_serv_t cclock;
    mach_timespec_t ts;

    host_get_clock_service(mach_host_self(), CALENDAR_CLOCK, &cclock);
    clock_get_time(cclock, &ts);
    mach_port_deallocate(mach_task_self(), cclock);
    return static_cast<uint64_t>(ts.tv_sec) * 1000000000 + ts.tv_nsec;
#else
    return std::chrono::duration_cast<std::chrono::nanoseconds>
              (std::chrono::steady_clock::now().time_since_epoch()).count();
#endif
  }

  virtual void SleepForMicroseconds(int micros) override {
    usleep(micros);
  }

  virtual Status GetHostName(char* name, uint64_t len) override {
    int ret = gethostname(name, static_cast<size_t>(len));
    if (ret < 0) {
      if (errno == EFAULT || errno == EINVAL) {
        return Status::InvalidArgument(strerror(errno));
      } else {
        return IOError("GetHostName", errno);
      }
    }
    return Status::OK();
  }

  virtual Status GetCurrentTime(int64_t* unix_time) override {
    time_t ret = time(nullptr);
    if (ret == (time_t) -1) {
      return IOError("GetCurrentTime", errno);
    }
    *unix_time = (int64_t) ret;
    return Status::OK();
  }

  virtual Status GetAbsolutePath(const std::string& db_path,
                                        std::string* output_path) override {
    *output_path = db_path;
    return Status::OK();
  }

  // Allow increasing the number of worker threads.
  virtual void SetBackgroundThreads(int num, Priority pri) override {
    assert(pri >= Priority::LOW && pri <= Priority::HIGH);
    thread_pools_[pri].SetBackgroundThreads(num);
  }

  // Allow increasing the number of worker threads.
  virtual void IncBackgroundThreadsIfNeeded(int num, Priority pri) override {
    assert(pri >= Priority::LOW && pri <= Priority::HIGH);
    thread_pools_[pri].IncBackgroundThreadsIfNeeded(num);
  }

  virtual void LowerThreadPoolIOPriority(Priority pool = LOW) override {
    assert(pool >= Priority::LOW && pool <= Priority::HIGH);

#ifdef OS_LINUX

    thread_pools_[pool].LowerIOPriority();

#endif
  }

  virtual std::string TimeToString(uint64_t secondsSince1970) override {
    const time_t seconds = (time_t)secondsSince1970;

    struct tm t;

    int maxsize = 64;

    std::string dummy;

    dummy.reserve(maxsize);
    dummy.resize(maxsize);

    char* p = &dummy[0];

    localtime_r(&seconds, &t);

    snprintf(p, maxsize, "%04d/%02d/%02d-%02d:%02d:%02d ",
             t.tm_year + 1900,
             t.tm_mon + 1,
             t.tm_mday,
             t.tm_hour,
             t.tm_min,
             t.tm_sec);
    return dummy;
  }

  EnvOptions OptimizeForLogWrite(const EnvOptions& env_options,
                                  const DBOptions& db_options) const override {
    EnvOptions optimized = env_options;
    optimized.use_mmap_writes = false;
    optimized.bytes_per_sync = db_options.wal_bytes_per_sync;
    // TODO(icanadi) it's faster if fallocate_with_keep_size is false, but it
    // breaks TransactionLogIteratorStallAtLastRecord unit test. Fix the unit
    // test and make this false
    optimized.fallocate_with_keep_size = true;
    optimized.type = kLogFile;
    return optimized;
  }

  EnvOptions OptimizeForManifestWrite(const EnvOptions& env_options) const override {
    EnvOptions optimized = env_options;
    optimized.type = kDescriptorFile;
    return optimized;
  }

  EnvOptions OptimizeForCurrentWrite(const EnvOptions& env_options) const override {
    EnvOptions optimized = env_options;
    optimized.type = kCurrentFile;
    return optimized;
  }

 private:
  nvm *nvm_api;

  nvm_directory *root_dir;

  const char *ftl_save_location = "root_nvm.layout";

  bool checkedDiskForMmap_;
  bool forceMmapOff; // do we override Env options?

  // Returns true iff the named directory exists and is a directory.
  virtual bool DirExists(const std::string& dname) {
    return (root_dir->OpenDirectory(dname.c_str()) != nullptr);
  }

  bool SupportsFastAllocate(const std::string& path) {
    return false;
  }

  size_t page_size_;

  std::vector<ThreadPool> thread_pools_;

  pthread_mutex_t mu_;
  std::vector<pthread_t> threads_to_join_;

  void LoadFtl() {
    int fd = open(ftl_save_location, O_RDONLY);

    char temp;

    if (fd < 0) {
      NVM_DEBUG("FTL file not found");
      return;
    }

    if (read(fd, &temp, 1) != 1) {
      NVM_DEBUG("FTL file is corrupt");
      close(fd);
      return;
    }

    if (temp != 'd') {
      NVM_DEBUG("FTL file is corrupt");
      close(fd);
      return;
    }

    NVM_DEBUG("read d. loading root directory");

    if (!root_dir->Load(fd).ok()) {
      NVM_DEBUG("FTL file is corrupt");

      delete root_dir;
      delete nvm_api;

      ALLOC_CLASS(nvm_api, nvm());
      ALLOC_CLASS(root_dir, nvm_directory("root", 4, nvm_api, nullptr));
    }

    close(fd);
  }
};

std::string Env::GenerateUniqueId() {
  // Could not read uuid_file - generate uuid using "nanos-random"
  Random64 r(time(nullptr));

  uint64_t random_uuid_portion = r.Uniform(std::numeric_limits<uint64_t>::max());
  uint64_t nanos_uuid_portion = NowNanos();

  char uuid2[200];

  snprintf(uuid2, 200, "%lx-%lx", (unsigned long)nanos_uuid_portion,
                                            (unsigned long)random_uuid_portion);
  return uuid2;
}

Env* Env::Default() {
  static NVMEnv default_env;
  return &default_env;
}

}  // namespace rocksdb

#endif

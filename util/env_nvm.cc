//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifdef ROCKSDB_PLATFORM_NVM

#include "nvm/nvm.h"

// This is only set from db_stress.cc and not used at the moment
int rocksdb_kill_odds = 0;

namespace rocksdb
{

struct StartThreadState
{
    void (*user_function)(void*);
    void* arg;
};

// list of pathnames that are locked
static std::set<std::string> lockedFiles;
static port::Mutex mutex_lockedFiles;

static void* StartThreadWrapper(void* arg)
{
    StartThreadState* state = reinterpret_cast<StartThreadState*>(arg);
    state->user_function(state->arg);

    delete state;
    return nullptr;
}

ThreadStatusUpdater* CreateThreadStatusUpdater()
{
    return new ThreadStatusUpdater();
}

static int LockOrUnlock(const std::string& fname, nvm_file *fd, bool lock)
{
    mutex_lockedFiles.Lock();
    if (lock)
    {
	// If it already exists in the lockedFiles set, then it is already locked,
	// and fail this lock attempt. Otherwise, insert it into lockedFiles.
	if (lockedFiles.insert(fname).second == false)
	{
	    mutex_lockedFiles.Unlock();
	    errno = ENOLCK;
	    return -1;
	}
    }
    else
    {
	// If we are unlocking, then verify that we had locked it earlier,
	// it should already exist in lockedFiles. Remove it from lockedFiles.
	if (lockedFiles.erase(fname) != 1)
	{
	    mutex_lockedFiles.Unlock();
	    errno = ENOLCK;
	    return -1;
	}
    }

    errno = 0;

    if(lock)
    {
	if(fd->LockFile())
	{
	    return -1;
	}
    }
    else
    {
	fd->UnlockFile();
    }

    mutex_lockedFiles.Unlock();
    return 0;
}

static void PthreadCall(const char* label, int result)
{
    if (result != 0)
    {
	fprintf(stderr, "pthread %s: %s\n", label, strerror(result));
	abort();
    }
}

//we only have 1 target device
//we need a mutex to control the file pointer
//should be removed when moving to a real machine
pthread_mutex_t rw_mtx;

class NVMEnv : public Env
{
    public:
	NVMEnv() :
	    checkedDiskForMmap_(false),
	    forceMmapOff(false),
	    page_size_(getpagesize()),
	    thread_pools_(Priority::TOTAL)
	{

	    PthreadCall("mutex_init", pthread_mutex_init(&mu_, nullptr));
	    for (int pool_id = 0; pool_id < Env::Priority::TOTAL; ++pool_id)
	    {
		thread_pools_[pool_id].SetThreadPriority(static_cast<Env::Priority>(pool_id));

		// This allows later initializing the thread-local-env of each thread.
		thread_pools_[pool_id].SetHostEnv(this);
	    }
	    thread_status_updater_ = CreateThreadStatusUpdater();

	    ALLOC_CLASS(nvm_api, nvm());
	    ALLOC_CLASS(file_manager, NVMFileManager(nvm_api))

	    pthread_mutex_init(&rw_mtx, nullptr);
	}

	virtual ~NVMEnv()
	{
	    for (const auto tid : threads_to_join_)
	    {
		pthread_join(tid, nullptr);
	    }

	    for (int pool_id = 0; pool_id < Env::Priority::TOTAL; ++pool_id)
	    {
		thread_pools_[pool_id].JoinAllThreads();
	    }

	    // All threads must be joined before the deletion of
	    // thread_status_updater_.
	    delete thread_status_updater_;
	    delete file_manager;
	    delete nvm_api;
	}

	virtual Status NewSequentialFile(const std::string& fname, unique_ptr<SequentialFile>* result, const EnvOptions& options) override
	{
	    result->reset();

	    nvm_file *f;

	    f = file_manager->nvm_fopen(fname.c_str(), "r");

	    if (f == nullptr)
	    {
		*result = nullptr;
		return IOError(fname, errno);
	    }
	    else
	    {
		result->reset(new NVMSequentialFile(fname, f, file_manager, nvm_api));
		return Status::OK();
	    }
	}

	virtual Status NewRandomAccessFile(const std::string& fname, unique_ptr<RandomAccessFile>* result, const EnvOptions& options) override
	{
	    result->reset();

	    nvm_file *f;

	    f = file_manager->nvm_fopen(fname.c_str(), "r");

	    if (f == nullptr)
	    {
		*result = nullptr;
		return IOError(fname, errno);
	    }
	    else
	    {
		result->reset(new NVMRandomAccessFile(fname, f, file_manager, nvm_api));
		return Status::OK();
	    }
	}

	virtual Status NewWritableFile(const std::string& fname, unique_ptr<WritableFile>* result, const EnvOptions& options) override
	{
	    result->reset();

	    Status s;

	    int fd = -1;

	    do
	    {
		fd = open(fname.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
	    } while (fd < 0 && errno == EINTR);

	    if (fd < 0)
	    {
		s = IOError(fname, errno);
	    }
	    else
	    {
		if (options.use_mmap_writes)
		{
		    if (!checkedDiskForMmap_)
		    {
			// this will be executed once in the program's lifetime.
			// do not use mmapWrite on non ext-3/xfs/tmpfs systems.
			if (!SupportsFastAllocate(fname))
			{
			    forceMmapOff = true;
			}
			checkedDiskForMmap_ = true;
		    }
		}
		if (options.use_mmap_writes && !forceMmapOff)
		{
		    result->reset(new NVMMmapFile(fname, fd, page_size_, options));
		}
		else
		{
		    // disable mmap writes
		    EnvOptions no_mmap_writes_options = options;
		    no_mmap_writes_options.use_mmap_writes = false;

		    result->reset(new NVMWritableFile(fname, fd, 65536, no_mmap_writes_options));
		}
	    }
	    return s;
	}

	virtual Status NewRandomRWFile(const std::string& fname, unique_ptr<RandomRWFile>* result, const EnvOptions& options) override
	{
	    result->reset();
	    // no support for mmap yet
	    if (options.use_mmap_writes || options.use_mmap_reads)
	    {
		return Status::NotSupported("No support for mmap read/write yet");
	    }

	    Status s;
	    const int fd = open(fname.c_str(), O_CREAT | O_RDWR, 0644);
	    if (fd < 0)
	    {
		s = IOError(fname, errno);
	    }
	    else
	    {
		result->reset(new NVMRandomRWFile(fname, fd, options));
	    }
	    return s;
	}

	virtual Status NewDirectory(const std::string& name, unique_ptr<Directory>* result) override
	{
	    result->reset();

	    nvm_directory *fd = file_manager->OpenDirectory(name.c_str());
	    if (fd == nullptr)
	    {
		return Status::IOError("directory doesn't exist");
	    }

	    result->reset(new NVMDirectory(fd));

	    return Status::OK();
	}

	virtual bool FileExists(const std::string& fname) override
	{
	    return file_manager->FileExists(fname.c_str());
	}

	virtual Status GetChildren(const std::string& dir, std::vector<std::string>* result) override
	{
	    result->clear();

	    DIR* d = opendir(dir.c_str());
	    if (d == nullptr)
	    {
		return IOError(dir, errno);
	    }

	    struct dirent* entry;
	    while ((entry = readdir(d)) != nullptr)
	    {
		result->push_back(entry->d_name);
	    }
	    closedir(d);
	    return Status::OK();
	}

	virtual Status DeleteFile(const std::string& fname) override
	{
	    if(file_manager->DeleteFile(fname.c_str()))
	    {
		return Status::IOError("delete file failed");
	    }

	    return Status::OK();
	}

	virtual Status CreateDir(const std::string& name) override
	{
	    if(file_manager->CreateDirectory(name.c_str()) == 0)
	    {
		return Status::OK();
	    }

	    return Status::IOError("createdir failed");
	}

	virtual Status CreateDirIfMissing(const std::string& name) override
	{
	    return CreateDir(name);
	}

	virtual Status DeleteDir(const std::string& name) override
	{
	    if(file_manager->DeleteDirectory(name.c_str()) == 0)
	    {
		return Status::OK();
	    }

	    return Status::IOError("delete dir failed");
	}

	virtual Status GetFileSize(const std::string& fname, uint64_t* size) override
	{
	    if(file_manager->GetFileSize(fname.c_str(), size))
	    {
		return Status::IOError("get file size failed");
	    }

	    return Status::OK();
	}

	virtual Status GetFileModificationTime(const std::string& fname, uint64_t* file_mtime) override
	{
	    if(file_manager->GetFileModificationTime(fname.c_str(), (time_t *)file_mtime))
	    {
		return Status::IOError("get file modification time failed");
	    }

	    return Status::OK();
	}

	virtual Status RenameFile(const std::string& src, const std::string& target) override
	{
	    if(file_manager->RenameFile(src.c_str(), target.c_str()))
	    {
		return Status::IOError("nvm rename file failed");
	    }

	    return Status::OK();
	}

	virtual Status LinkFile(const std::string& src, const std::string& target) override
	{
	    if (file_manager->LinkFile(src.c_str(), target.c_str()) != 0)
	    {
		return Status::IOError("nvm link file failed");
	    }
	    return Status::OK();
	}

	virtual Status LockFile(const std::string& fname, FileLock** lock) override
	{
	    *lock = nullptr;

	    nvm_file *f;

	    Status result;

	    f = file_manager->nvm_fopen(fname.c_str(), "w");

	    if (LockOrUnlock(fname, f, true) == -1)
	    {
		result = IOError("lock " + fname, errno);
		file_manager->nvm_fclose(f);
	    }
	    else
	    {
		NVMFileLock *my_lock = new NVMFileLock;
		my_lock->fd_ = f;
		my_lock->filename = fname;
		*lock = my_lock;
	    }

	    return result;
	}

	virtual Status UnlockFile(FileLock* lock) override
	{
	    NVMFileLock* my_lock = reinterpret_cast<NVMFileLock*>(lock);

	    Status result;

	    if (LockOrUnlock(my_lock->filename, my_lock->fd_, false) == -1)
	    {
		result = IOError("unlock", errno);
	    }

	    file_manager->nvm_fclose(my_lock->fd_);

	    delete my_lock;
	    return result;
	}

	virtual void Schedule(void (*function)(void* arg1), void* arg, Priority pri = LOW, void* tag = nullptr) override
	{
	    assert(pri >= Priority::LOW && pri <= Priority::HIGH);
	    thread_pools_[pri].Schedule(function, arg, tag);
	}

	virtual int UnSchedule(void* arg, Priority pri) override
	{
	    return thread_pools_[pri].UnSchedule(arg);
	}

	virtual void StartThread(void (*function)(void* arg), void* arg) override
	{
	    pthread_t t;

	    StartThreadState* state = new StartThreadState;
	    state->user_function = function;
	    state->arg = arg;

	    PthreadCall("start thread", pthread_create(&t, nullptr,  &StartThreadWrapper, state));
	    PthreadCall("lock", pthread_mutex_lock(&mu_));

	    threads_to_join_.push_back(t);

	    PthreadCall("unlock", pthread_mutex_unlock(&mu_));
	}

	virtual void WaitForJoin() override
	{
	    for (const auto tid : threads_to_join_)
	    {
		pthread_join(tid, nullptr);
	    }
	    threads_to_join_.clear();
	}

	virtual unsigned int GetThreadPoolQueueLen(Priority pri = LOW) const override
	{
	    assert(pri >= Priority::LOW && pri <= Priority::HIGH);
	    return thread_pools_[pri].GetQueueLen();
	}

	virtual Status GetTestDirectory(std::string* result) override
	{
	    return Status::IOError("Not supported");
	}

	virtual Status GetThreadList(std::vector<ThreadStatus>* thread_list) override
	{
	    assert(thread_status_updater_);
	    return thread_status_updater_->GetThreadList(thread_list);
	}

	static uint64_t gettid(pthread_t tid)
	{
	    uint64_t thread_id = 0;
	    memcpy(&thread_id, &tid, std::min(sizeof(thread_id), sizeof(tid)));
	    return thread_id;
	}

	static uint64_t gettid()
	{
	    pthread_t tid = pthread_self();
	    return gettid(tid);
	}

	virtual Status NewLogger(const std::string& fname, shared_ptr<Logger>* result) override
	{
	    FILE* f = fopen(fname.c_str(), "w");
	    if (f == nullptr)
	    {
		result->reset();
		return IOError(fname, errno);
	    }
	    else
	    {
		result->reset(new NVMLogger(f, &NVMEnv::gettid, this));
		return Status::OK();
	    }
	}

	virtual uint64_t NowMicros() override
	{
	    struct timeval tv;
	    gettimeofday(&tv, nullptr);
	    return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
	}

	virtual uint64_t NowNanos() override
	{
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

	    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();

#endif
	}

	virtual void SleepForMicroseconds(int micros) override
	{
	    usleep(micros);
	}

	virtual Status GetHostName(char* name, uint64_t len) override
	{
	    int ret = gethostname(name, static_cast<size_t>(len));
	    if (ret < 0)
	    {
		if (errno == EFAULT || errno == EINVAL)
		{
		    return Status::InvalidArgument(strerror(errno));
		}
		else
		{
		    return IOError("GetHostName", errno);
		}
	    }
	    return Status::OK();
	}

	virtual Status GetCurrentTime(int64_t* unix_time) override
	{
	    time_t ret = time(nullptr);
	    if (ret == (time_t) -1)
	    {
		return IOError("GetCurrentTime", errno);
	    }
	    *unix_time = (int64_t) ret;
	    return Status::OK();
	}

	virtual Status GetAbsolutePath(const std::string& db_path, std::string* output_path) override
	{
	    const char *nvm_api_location = nvm_api->GetLocation();

	    if (db_path.find(nvm_api_location) == 0)
	    {
		*output_path = db_path;
		return Status::OK();
	    }

	    *output_path = std::string(nvm_api_location) + std::string("/") + db_path;
	    return Status::OK();
	}

	// Allow increasing the number of worker threads.
	virtual void SetBackgroundThreads(int num, Priority pri) override
	{
	    assert(pri >= Priority::LOW && pri <= Priority::HIGH);
	    thread_pools_[pri].SetBackgroundThreads(num);
	}

	// Allow increasing the number of worker threads.
	virtual void IncBackgroundThreadsIfNeeded(int num, Priority pri) override
	{
	    assert(pri >= Priority::LOW && pri <= Priority::HIGH);
	    thread_pools_[pri].IncBackgroundThreadsIfNeeded(num);
	}

	virtual void LowerThreadPoolIOPriority(Priority pool = LOW) override
	{
	    assert(pool >= Priority::LOW && pool <= Priority::HIGH);

#ifdef OS_LINUX

	    thread_pools_[pool].LowerIOPriority();

#endif
	}

	virtual std::string TimeToString(uint64_t secondsSince1970) override
	{
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

	EnvOptions OptimizeForLogWrite(const EnvOptions& env_options, const DBOptions& db_options) const override
	{
	    EnvOptions optimized = env_options;
	    optimized.use_mmap_writes = false;
	    optimized.bytes_per_sync = db_options.wal_bytes_per_sync;
	    // TODO(icanadi) it's faster if fallocate_with_keep_size is false, but it
	    // breaks TransactionLogIteratorStallAtLastRecord unit test. Fix the unit
	    // test and make this false
	    optimized.fallocate_with_keep_size = true;
	    return optimized;
	}

	EnvOptions OptimizeForManifestWrite(const EnvOptions& env_options) const override
	{
	    EnvOptions optimized = env_options;

	    optimized.use_mmap_writes = false;
	    optimized.fallocate_with_keep_size = true;

	    return optimized;
	}

    private:
	nvm *nvm_api;

	NVMFileManager *file_manager;

	bool checkedDiskForMmap_;
	bool forceMmapOff; // do we override Env options?

	// Returns true iff the named directory exists and is a directory.
	virtual bool DirExists(const std::string& dname)
	{
	    return (file_manager->OpenDirectory(dname.c_str()) != nullptr);
	}

	bool SupportsFastAllocate(const std::string& path)
	{
#ifdef ROCKSDB_FALLOCATE_PRESENT

	    struct statfs s;
	    if (statfs(path.c_str(), &s))
	    {
		return false;
	    }

	    switch (s.f_type)
	    {
		case EXT4_SUPER_MAGIC:
		{
		}
		return true;

		case XFS_SUPER_MAGIC:
		{
		}
		return true;

		case TMPFS_MAGIC:
		{
		}
		return true;

		default:
		{
		}
		return false;
	    }
#else

	    return false;
#endif
	}

	size_t page_size_;	

	std::vector<ThreadPool> thread_pools_;

	pthread_mutex_t mu_;
	std::vector<pthread_t> threads_to_join_;
};

std::string Env::GenerateUniqueId()
{
    std::string uuid_file = "/proc/sys/kernel/random/uuid";
    if (FileExists(uuid_file))
    {
	std::string uuid;
	Status s = ReadFileToString(this, uuid_file, &uuid);
	if (s.ok())
	{
	    return uuid;
	}
    }

    // Could not read uuid_file - generate uuid using "nanos-random"
    Random64 r(time(nullptr));

    uint64_t random_uuid_portion = r.Uniform(std::numeric_limits<uint64_t>::max());
    uint64_t nanos_uuid_portion = NowNanos();

    char uuid2[200];

    snprintf(uuid2, 200, "%lx-%lx", (unsigned long)nanos_uuid_portion, (unsigned long)random_uuid_portion);
    return uuid2;
}

Env* Env::Default()
{
    static NVMEnv default_env;
    return &default_env;
}

}  // namespace rocksdb

#endif

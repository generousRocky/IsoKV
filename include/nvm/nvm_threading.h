#ifndef _NVM_THREADING_H_
#define _NVM_THREADING_H_

#include "nvm.h"

namespace rocksdb
{

// Entry per Schedule() call
struct BGItem
{
    void* arg;
    void (*function)(void*);
    void* tag;
};

class ThreadPool
{
    private:
	typedef std::deque<BGItem> BGQueue;

	pthread_mutex_t mu_;

	pthread_cond_t bgsignal_;

	int total_threads_limit_;

	std::vector<pthread_t> bgthreads_;

	BGQueue queue_;

	std::atomic_uint queue_len_;  // Queue length. Used for stats reporting

	bool exit_all_threads_;
	bool low_io_priority_;

	Env::Priority priority_;

	Env* env_;

    public:
	ThreadPool();
	~ThreadPool();

	void JoinAllThreads();

	void SetHostEnv(Env* env);

	void LowerIOPriority();

	// Return true if there is at least one thread needs to terminate.
	bool HasExcessiveThread();

	// Return true iff the current thread is the excessive thread to terminate.
	// Always terminate the running thread that is added last, even if there are
	// more than one thread to terminate.
	bool IsLastExcessiveThread(size_t thread_id);

	// Is one of the threads to terminate.
	bool IsExcessiveThread(size_t thread_id);

	// Return the thread priority.
	// This would allow its member-thread to know its priority.
	Env::Priority GetThreadPriority();

	// Set the thread priority.
	void SetThreadPriority(Env::Priority priority);

	void BGThread(size_t thread_id);

	static void* BGThreadWrapper(void* arg);

	void WakeUpAllThreads();

	void SetBackgroundThreadsInternal(int num, bool allow_reduce);

	void IncBackgroundThreadsIfNeeded(int num);

	void SetBackgroundThreads(int num);

	void StartBGThreads();

	void Schedule(void (*function)(void* arg1), void* arg, void* tag);

	int UnSchedule(void* arg);

	unsigned int GetQueueLen() const;
};

// Helper struct for passing arguments when creating threads.
struct BGThreadMetadata
{
    ThreadPool* thread_pool_;

    size_t thread_id_;  // Thread count in the thread.
    explicit BGThreadMetadata(ThreadPool* thread_pool, size_t thread_id) : thread_pool_(thread_pool), thread_id_(thread_id)
    {}
};

}

#endif

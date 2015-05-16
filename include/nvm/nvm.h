#ifndef _NVM_H_
#define _NVM_H_

#include <iostream>
#include <atomic>
#include <deque>
#include <set>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/stat.h>

#ifdef OS_LINUX

#include <sys/statfs.h>
#include <sys/syscall.h>

#endif

#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#if defined(OS_LINUX)

#include <linux/fs.h>

#endif

#include <signal.h>
#include <algorithm>
#include "rocksdb/env.h"
#include "nvm_debug.h"
#include "nvm_mem.h"
#include "nvm_ioctl.h"
#include "nvm_typedefs.h"
#include "nvm_files.h"
#include "nvm_threading.h"
#include "rocksdb/slice.h"
#include "port/port.h"
#include "util/coding.h"
#include "util/logging.h"
#include "util/logger.h"
#include "util/random.h"
#include "util/iostats_context_imp.h"
#include "util/rate_limiter.h"
#include "util/sync_point.h"
#include "util/thread_status_updater.h"
#include "util/thread_status_util.h"

// Get nano time includes
#if defined(OS_LINUX) || defined(OS_FREEBSD)

#elif defined(__MACH__)

#include <mach/clock.h>
#include <mach/mach.h>

#else

#include <chrono>

#endif

#if !defined(TMPFS_MAGIC)

#define TMPFS_MAGIC 0x01021994

#endif

#if !defined(XFS_SUPER_MAGIC)

#define XFS_SUPER_MAGIC 0x58465342

#endif

#if !defined(EXT4_SUPER_MAGIC)

#define EXT4_SUPER_MAGIC 0xEF53

#endif

// For non linux platform, the following macros are used only as place
// holder.
#if !(defined OS_LINUX) && !(defined CYGWIN)

#define NVM_FADV_NORMAL	    0 /* [MC1] no further special treatment */
#define NVM_FADV_RANDOM	    1 /* [MC1] expect random page refs */
#define NVM_FADV_SEQUENTIAL 2 /* [MC1] expect sequential page refs */
#define NVM_FADV_WILLNEED   3 /* [MC1] will need these pages */
#define NVM_FADV_DONTNEED   4 /* [MC1] dont need these pages */

#else

#define NVM_FADV_NORMAL	    POSIX_FADV_NORMAL	    /* [MC1] no further special treatment */
#define NVM_FADV_RANDOM	    POSIX_FADV_RANDOM	    /* [MC1] expect random page refs */
#define NVM_FADV_SEQUENTIAL POSIX_FADV_SEQUENTIAL   /* [MC1] expect sequential page refs */
#define NVM_FADV_WILLNEED   POSIX_FADV_WILLNEED	    /* [MC1] will need these pages */
#define NVM_FADV_DONTNEED   POSIX_FADV_DONTNEED	    /* [MC1] dont need these pages */

#endif

inline rocksdb::Status IOError(const std::string& context, int err_number)
{
    return rocksdb::Status::IOError(context, strerror(err_number));
}

#endif

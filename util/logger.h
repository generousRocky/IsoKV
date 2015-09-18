#pragma once

#if defined(ROCKSDB_PLATFORM_POSIX)

#include "util/posix_logger.h"

#elif defined (ROCKSDB_PLATFORM_NVM)

#include "util/posix_logger.h"
// #include "util/nvm_logger.h"

#else

#error "unknown platform"

#endif

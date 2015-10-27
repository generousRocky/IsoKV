#ifndef _DEBUG_DFLASH_H_
#define _DEBUG_DFLASH_H_

#include <stdio.h>

#define DFLASH_DEBUG_ENABLED

#define DFLASH_ASSERT(c, x, ...) if(!(c)){printf("%s:%s - %d %s" x "\n", __FILE__, __FUNCTION__, __LINE__, strerror(errno), ##__VA_ARGS__);fflush(stdout);exit(EXIT_FAILURE);}
#define DFLASH_ERROR(x, ...) printf("%s:%s - %d %s" x "\n", __FILE__, __FUNCTION__, __LINE__, strerror(errno), ##__VA_ARGS__);fflush(stdout);
#define DFLASH_FATAL(x, ...) printf("%s:%s - %d %s" x "\n", __FILE__, __FUNCTION__, __LINE__, strerror(errno), ##__VA_ARGS__);fflush(stdout);exit(EXIT_FAILURE)

#ifdef DFLASH_DEBUG_ENABLED
  #define DFLASH_DEBUG(x, ...) printf("%s:%s - %d " x "\n", __FILE__, __FUNCTION__, __LINE__, ##__VA_ARGS__);fflush(stdout);
#else
  #define DFLASH_DEBUG(x, ...)
#endif
#endif // _DEBUG_DFLASH_H_

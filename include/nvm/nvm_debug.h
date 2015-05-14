#ifndef _NVM_DEBUG_H_
#define _NVM_DEBUG_H_

#include <stdio.h>

#define NVM_DEBUG_ENABLED

#define NVM_ERROR(x, ...) printf("%s:%s - %d %s" x "\n", __FILE__, __FUNCTION__, __LINE__, strerror(errno), ##__VA_ARGS__);

#ifdef NVM_DEBUG_ENABLED

#define NVM_DEBUG(x, ...) printf("%s:%s - %d " x "\n", __FILE__, __FUNCTION__, __LINE__, ##__VA_ARGS__);


#else

#define NVM_DEBUG(x, ...)

#endif

#endif

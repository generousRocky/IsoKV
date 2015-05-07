#ifndef _NVM_DEBUG_H_
#define _NVM_DEBUG_H_

#include <stdio.h>

#define NVM_DEBUG(x, ...) printf("%s:%s - %d " x "\n", __FILE__, __FUNCTION__, __LINE__, ##__VA_ARGS__);

#endif

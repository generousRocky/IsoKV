#ifdef ROCKSDB_PLATFORM_NVM

#include "nvm/nvm.h"

using namespace rocksdb;

int main(int argc, char **argv)
{
    return 0;
}

#else

int main(void)
{
    return 0;
}

#endif

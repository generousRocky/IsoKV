#ifdef ROCKSDB_PLATFORM_NVM

#include "nvm/nvm.h"

namespace rocksdb
{

nvm_directory::nvm_directory(const char *_name)
{
    SAFE_ALLOC(name, char[strlen(_name + 1)]);
    strcpy(name, _name);
}

nvm_directory::~nvm_directory()
{
    delete[] name;
}

bool nvm_directory::HasName(const char *_name)
{
    return (strcmp(name, _name) == 0);
}

}

#endif

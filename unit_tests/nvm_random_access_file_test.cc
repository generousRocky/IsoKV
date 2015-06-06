#ifdef ROCKSDB_PLATFORM_NVM

#include "nvm/nvm.h"

using namespace rocksdb;

int main(int argc, char **argv)
{
    NVMFileManager *file_manager;
    nvm *nvm_api;

    ALLOC_CLASS(nvm_api, nvm());
    ALLOC_CLASS(file_manager, NVMFileManager(nvm_api));

    NVM_DEBUG("init complete");

    nvm_file *fd = file_manager->nvm_fopen("test.c", "w");

    NVM_DEBUG("file open at %p", fd);

    fd->make_dummy(nvm_api);

    NVMRandomAccessFile *test_file = new NVMRandomAccessFile("test.c", fd, file_manager, nvm_api);

    unsigned long size = fd->GetSize();

    NVM_DEBUG("file has size %lu", size);

    Slice r;

    char *scratch;

    scratch = new char[size / 4 + 1];

    if(!test_file->Read(size / 2, size / 4, &r, scratch).ok())
    {
	NVM_FATAL("read test failed");
    }

    NVM_DEBUG("read ok");

    if(!test_file->Read(size, size / 4, &r, scratch).ok())
    {
	NVM_FATAL("read 2 test failed");
    }

    NVM_DEBUG("read 2 ok");
}

#endif

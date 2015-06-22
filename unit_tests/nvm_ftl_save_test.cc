#ifdef ROCKSDB_PLATFORM_NVM

#include "nvm/nvm.h"

using namespace rocksdb;

int main(int argc, char **argv)
{
    nvm_directory *dir;
    nvm *nvm_api;

    ALLOC_CLASS(nvm_api, nvm());
    ALLOC_CLASS(dir, nvm_directory("root", 4, nvm_api, nullptr));

    dir->CreateDirectory("test");
    dir->CreateDirectory("test1");
    dir->CreateDirectory("test2");

    nvm_directory *dir1 = dir->OpenDirectory("test1");

    dir1->CreateDirectory("testx");
    dir1->CreateDirectory("testxx");
    dir1->CreateDirectory("testxxx");

    int fd = open("root_nvm.layout", O_RDWR | O_CREAT, S_IWUSR | S_IRUSR);

    if(fd < 0)
    {
	NVM_FATAL("");
    }

    if(!dir->Save(fd, 0).ok())
    {
	NVM_FATAL("");
    }

    close(fd);

    return 0;
}

#else

int main(void)
{
    return 0;
}

#endif

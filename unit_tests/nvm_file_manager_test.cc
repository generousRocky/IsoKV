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

    nvm_file *open1 = file_manager->nvm_fopen("test.c", "r");

    if(open1 != NULL)
    {
	NVM_FATAL("");
    }

    open1 = file_manager->nvm_fopen("test.c", "r");

    if(open1 != NULL)
    {
	NVM_FATAL("");
    }

    nvm_file *open2 = file_manager->nvm_fopen("test.c", "w");

    if(open2 == NULL)
    {
	NVM_FATAL("");
    }

    nvm_file *open3 = file_manager->nvm_fopen("test.c", "w");

    if(open3 == NULL)
    {
	NVM_FATAL("");
    }

    if(open2 != open3)
    {
	NVM_FATAL("");
    }

    file_manager->nvm_fclose(open2);
    file_manager->nvm_fclose(open3);

    open1 = file_manager->nvm_fopen("test1.c", "w");
    if(open1 == NULL)
    {
	NVM_FATAL("");
    }
    file_manager->nvm_fclose(open1);

    open1 = file_manager->nvm_fopen("test2.c", "w");
    if(open1 == NULL)
    {
	NVM_FATAL("");
    }
    file_manager->nvm_fclose(open1);

    open1 = file_manager->nvm_fopen("test3.c", "w");
    if(open1 == NULL)
    {
	NVM_FATAL("");
    }
    file_manager->nvm_fclose(open1);

    open1 = file_manager->nvm_fopen("test4.c", "w");
    if(open1 == NULL)
    {
	NVM_FATAL("");
    }
    file_manager->nvm_fclose(open1);

    open1 = file_manager->nvm_fopen("test1.c", "r");
    if(open1 == NULL)
    {
	NVM_FATAL("");
    }
    file_manager->nvm_fclose(open1);

    delete file_manager;
    delete nvm_api;

    return 0;
}

#endif

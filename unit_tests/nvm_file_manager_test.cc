#ifdef ROCKSDB_PLATFORM_NVM

#include "nvm/nvm.h"

using namespace rocksdb;

void TestOpenAndClose(NVMFileManager *file_manager, nvm *nvm_api)
{
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
}

void TestFileSize(NVMFileManager *file_manager, nvm *nvm_api)
{
    unsigned long size;

    if(file_manager->GetFileSize("bla.c", &size) == 0)
    {
	NVM_FATAL("");
    }

    NVM_DEBUG("size is %lu", size);

    if(file_manager->GetFileSize("test.c", &size) != 0)
    {
	NVM_FATAL("");
    }

    NVM_DEBUG("size is %lu", size);

    nvm_file *open1 = file_manager->nvm_fopen("test.c", "w");

    if(open1 != NULL)
    {
	NVM_FATAL("");
    }
    open1->make_dummy(nvm_api);
    file_manager->nvm_fclose(open1);

    if(file_manager->GetFileSize("test.c", &size) != 0)
    {
	NVM_FATAL("");
    }

    NVM_DEBUG("size is %lu", size);
}

void TestFileDelete(NVMFileManager *file_manager, nvm *nvm_api)
{
    file_manager->DeleteFile("bla.c");

    NVM_DEBUG("delete bla.c ok");

    nvm_file *open1 = file_manager->nvm_fopen("test.c", "w");

    if(open1 == NULL)
    {
	NVM_FATAL("");
    }
    open1->make_dummy(nvm_api);
    file_manager->nvm_fclose(open1);

    open1 = file_manager->nvm_fopen("test1.c", "w");

    if(open1 == NULL)
    {
	NVM_FATAL("");
    }
    file_manager->nvm_fclose(open1);

    file_manager->DeleteFile("test.c");

    NVM_DEBUG("delete test.c ok");

    file_manager->DeleteFile("test1.c");

    NVM_DEBUG("delete test1.c ok");

    file_manager->DeleteFile("test2.c");

    NVM_DEBUG("delete test2.c ok");

    open1 = file_manager->nvm_fopen("test.c", "r");

    if(open1 != NULL)
    {
	NVM_FATAL("");
    }

    open1 = file_manager->nvm_fopen("test1.c", "r");

    if(open1 != NULL)
    {
	NVM_FATAL("");
    }

    open1 = file_manager->nvm_fopen("test2.c", "r");

    if(open1 != NULL)
    {
	NVM_FATAL("");
    }

    NVM_DEBUG("delete tests done");
}

void TestFileModification(NVMFileManager *file_manager, nvm *nvm_api)
{
    nvm_file *open1 = file_manager->nvm_fopen("test1.c", "w");

    if(open1 == NULL)
    {
	NVM_FATAL("");
    }

    unsigned long last_modified;

    if(file_manager->GetFileModificationTime("test2.c", (time_t *)&last_modified) == 0)
    {
	NVM_FATAL("");
    }

    NVM_DEBUG("test2 ok");

    if(file_manager->GetFileModificationTime("test1.c", (time_t *)&last_modified))
    {
	NVM_FATAL("");
    }

    NVM_DEBUG("test1 ok: %lu", last_modified);

    open1->UpdateFileModificationTime();

    if(file_manager->GetFileModificationTime("test1.c", (time_t *)&last_modified))
    {
	NVM_FATAL("");
    }

    NVM_DEBUG("test1 ok: %lu", last_modified);

    file_manager->nvm_fclose(open1);
}

int main(int argc, char **argv)
{
    NVMFileManager *file_manager;
    nvm *nvm_api;

    ALLOC_CLASS(nvm_api, nvm());
    ALLOC_CLASS(file_manager, NVMFileManager(nvm_api));

    NVM_DEBUG("init complete");

    //TestOpenAndClose(file_manager, nvm_api);

    //TestFileSize(file_manager, nvm_api);

    //TestFileDelete(file_manager, nvm_api);

    TestFileModification(file_manager, nvm_api);

    delete file_manager;
    delete nvm_api;

    return 0;
}

#endif

#ifdef ROCKSDB_PLATFORM_NVM

#include <iostream>
#include "nvm/nvm.h"

using namespace rocksdb;

void TestFileExists(nvm_directory *dir, nvm *nvm_api)
{
    if(dir->FileExists("test.c"))
    {
	NVM_FATAL("");
    }

    dir->nvm_fopen("test.c", "w");

    if(!dir->FileExists("test.c"))
    {
	NVM_FATAL("");
    }

    dir->nvm_fopen("test1.c", "w");

    if(!dir->FileExists("test.c"))
    {
	NVM_FATAL("");
    }

    if(!dir->FileExists("test1.c"))
    {
	NVM_FATAL("");
    }

    dir->DeleteFile("test.c");

    if(dir->FileExists("test.c"))
    {
	NVM_FATAL("");
    }

    dir->DeleteFile("test1.c");

    if(dir->FileExists("test1.c"))
    {
	NVM_FATAL("");
    }

    if(dir->FileExists("test.c"))
    {
	NVM_FATAL("");
    }
}

void TestOpenAndClose(nvm_directory *dir, nvm *nvm_api)
{
    nvm_file *open1 = dir->nvm_fopen("test.c", "r");

    if(open1 != NULL)
    {
	NVM_FATAL("");
    }

    open1 = dir->nvm_fopen("test.c", "r");

    if(open1 != NULL)
    {
	NVM_FATAL("");
    }

    nvm_file *open2 = dir->nvm_fopen("test.c", "w");

    if(open2 == NULL)
    {
	NVM_FATAL("");
    }

    nvm_file *open3 = dir->nvm_fopen("test.c", "w");

    if(open3 != NULL)
    {
	NVM_FATAL("");
    }

    open3 = dir->nvm_fopen("test.c", "r");

    if(open3 == NULL)
    {
	NVM_FATAL("");
    }

    if(open2 != open3)
    {
	NVM_FATAL("");
    }

    dir->nvm_fclose(open2, "w");
    dir->nvm_fclose(open3, "r");

    open1 = dir->nvm_fopen("test1.c", "w");
    if(open1 == NULL)
    {
	NVM_FATAL("");
    }
    dir->nvm_fclose(open1, "w");

    open1 = dir->nvm_fopen("test2.c", "w");
    if(open1 == NULL)
    {
	NVM_FATAL("");
    }
    dir->nvm_fclose(open1, "w");

    open1 = dir->nvm_fopen("test3.c", "w");
    if(open1 == NULL)
    {
	NVM_FATAL("");
    }
    dir->nvm_fclose(open1, "w");

    open1 = dir->nvm_fopen("test4.c", "w");
    if(open1 == NULL)
    {
	NVM_FATAL("");
    }
    dir->nvm_fclose(open1, "w");

    open1 = dir->nvm_fopen("test1.c", "r");
    if(open1 == NULL)
    {
	NVM_FATAL("");
    }
    dir->nvm_fclose(open1, "r");
}

void TestFileSize(nvm_directory *dir, nvm *nvm_api)
{
    unsigned long size;

    if(dir->GetFileSize("bla.c", &size) == 0)
    {
	NVM_FATAL("");
    }

    NVM_DEBUG("size is %lu", size);

    dir->nvm_fopen("test.c", "w");

    if(dir->GetFileSize("test.c", &size) != 0)
    {
	NVM_FATAL("");
    }

    NVM_DEBUG("size is %lu", size);

    nvm_file *open1 = dir->nvm_fopen("test.c", "w");

    if(open1 != NULL)
    {
	NVM_FATAL("");
    }

    if(dir->GetFileSize("test.c", &size) != 0)
    {
	NVM_FATAL("");
    }

    NVM_DEBUG("size is %lu", size);
}

void TestFileDelete(nvm_directory *dir, nvm *nvm_api)
{
    dir->DeleteFile("bla.c");

    NVM_DEBUG("delete bla.c ok");

    nvm_file *open1 = dir->nvm_fopen("test.c", "w");

    if(open1 == NULL)
    {
	NVM_FATAL("");
    }
    dir->nvm_fclose(open1, "w");

    open1 = dir->nvm_fopen("test1.c", "w");

    if(open1 == NULL)
    {
	NVM_FATAL("");
    }
    dir->nvm_fclose(open1, "w");

    dir->DeleteFile("test.c");

    NVM_DEBUG("delete test.c ok");

    dir->DeleteFile("test1.c");

    NVM_DEBUG("delete test1.c ok");

    dir->DeleteFile("test2.c");

    NVM_DEBUG("delete test2.c ok");

    open1 = dir->nvm_fopen("test.c", "r");

    if(open1 != NULL)
    {
	NVM_FATAL("");
    }

    open1 = dir->nvm_fopen("test1.c", "r");

    if(open1 != NULL)
    {
	NVM_FATAL("");
    }

    open1 = dir->nvm_fopen("test2.c", "r");

    if(open1 != NULL)
    {
	NVM_FATAL("");
    }

    NVM_DEBUG("delete tests done");
}

void TestFileModification(nvm_directory *dir, nvm *nvm_api)
{
    nvm_file *open1 = dir->nvm_fopen("test1.c", "w");

    if(open1 == NULL)
    {
	NVM_FATAL("");
    }

    unsigned long last_modified;

    if(dir->GetFileModificationTime("test2.c", (time_t *)&last_modified) == 0)
    {
	NVM_FATAL("");
    }

    NVM_DEBUG("test2 ok");

    if(dir->GetFileModificationTime("test1.c", (time_t *)&last_modified))
    {
	NVM_FATAL("");
    }

    NVM_DEBUG("test1 ok: %lu", last_modified);

    open1->UpdateFileModificationTime();

    if(dir->GetFileModificationTime("test1.c", (time_t *)&last_modified))
    {
	NVM_FATAL("");
    }

    NVM_DEBUG("test1 ok: %lu", last_modified);

    dir->nvm_fclose(open1, "w");
}

void TestFileRename(nvm_directory *dir, nvm *nvm_api)
{
    nvm_file *open1 = dir->nvm_fopen("test1.c", "w");

    if(open1 == NULL)
    {
	NVM_FATAL("");
    }

    dir->nvm_fclose(open1, "w");

    open1 = dir->nvm_fopen("test2.c", "w");

    if(open1 == NULL)
    {
	NVM_FATAL("");
    }

    dir->nvm_fclose(open1, "w");

    if(dir->RenameFile("test1.c", "test2.c") != 0)
    {
	NVM_FATAL("");
    }

    if(dir->RenameFile("test1.c", "test3.c") == 0)
    {
	NVM_FATAL("");
    }

    open1 = dir->nvm_fopen("test1.c", "r");

    if(open1 != NULL)
    {
	NVM_FATAL("");
    }

    open1 = dir->nvm_fopen("test3.c", "r");

    if(open1 != NULL)
    {
	NVM_FATAL("");
    }

    dir->nvm_fclose(open1, "r");

    if(dir->CreateDirectory("testd1") != 0)
    {
	NVM_FATAL("");
    }

    if(dir->CreateDirectory("testd2") != 0)
    {
	NVM_FATAL("");
    }

    open1 = dir->nvm_fopen("testd1/da1", "w");

    if(open1 == nullptr)
    {
	NVM_FATAL("");
    }

    if(dir->RenameFile("testd1/da1", "testd2/da4") != 0)
    {
	NVM_FATAL("");
    }

    open1 = dir->nvm_fopen("testd1/da1", "r");

    if(open1 != nullptr)
    {
	NVM_FATAL("");
    }

    open1 = dir->nvm_fopen("testd2/da4", "r");

    if(open1 == nullptr)
    {
	NVM_FATAL("");
    }

    NVM_DEBUG("rename file test passed");
}

void TestFileLinkUnlink(nvm_directory *dir, nvm *nvm_api)
{
    nvm_file *open1 = dir->nvm_fopen("test1.c", "w");

    if(open1 == NULL)
    {
	NVM_FATAL("");
    }

    nvm_file *open2 = dir->nvm_fopen("test2.c", "w");

    if(open2 == NULL)
    {
	NVM_FATAL("");
    }

    int ret = dir->LinkFile("test1.c", "test2.c");

    if(ret == 0)
    {
	NVM_FATAL("");
    }

    ret = dir->LinkFile("test2.c", "test1.c");

    if(ret == 0)
    {
	NVM_FATAL("");
    }

    ret = dir->LinkFile("test1.c", "test3.c");

    if(ret != 0)
    {
	NVM_FATAL("");
    }

    nvm_file *open3 = dir->nvm_fopen("test3.c", "r");

    if(open3 == NULL)
    {
	NVM_FATAL("");
    }

    if(open3 != open1)
    {
	NVM_FATAL("");
    }

    dir->nvm_fclose(open1, "w");
    dir->nvm_fclose(open2, "w");
    dir->nvm_fclose(open3, "r");

    ret = dir->RenameFile("test3.c", "test31.c");

    open1 = dir->nvm_fopen("test1.c", "r");

    if(open1 == NULL)
    {
	NVM_FATAL("");
    }

    open2 = dir->nvm_fopen("test2.c", "r");

    if(open2 == NULL)
    {
	NVM_FATAL("");
    }

    ret = dir->LinkFile("test2.c", "test31.c");

    if(ret == 0)
    {
	NVM_FATAL("");
    }

    ret = dir->LinkFile("test2.c", "test3.c");

    if(ret != 0)
    {
	NVM_FATAL("");
    }

    open3 = dir->nvm_fopen("test3.c", "r");

    if(open3 == NULL)
    {
	NVM_FATAL("");
    }

    if(open2 != open3)
    {
	NVM_FATAL("");
    }

    dir->nvm_fclose(open1, "r");
    dir->nvm_fclose(open2, "r");
    dir->nvm_fclose(open3, "r");

    dir->DeleteFile("test1.c");
    dir->DeleteFile("test2.c");
    dir->DeleteFile("test3.c");
    dir->DeleteFile("test31.c");

    open1 = dir->nvm_fopen("test1.c", "r");

    if(open1 != NULL)
    {
	NVM_FATAL("");
    }

    open1 = dir->nvm_fopen("test2.c", "r");

    if(open1 != NULL)
    {
	NVM_FATAL("");
    }

    open1 = dir->nvm_fopen("test3.c", "r");

    if(open1 != NULL)
    {
	NVM_FATAL("");
    }

    open1 = dir->nvm_fopen("test31.c", "r");

    if(open1 != NULL)
    {
	NVM_FATAL("");
    }
}

void TestDirectoryCreate(nvm_directory *dir, nvm *nvm_api)
{
    nvm_directory *dir1 = dir->OpenDirectory("test");

    if(dir1)
    {
	NVM_FATAL("");
    }

    if(dir->CreateDirectory("test") != 0)
    {
	NVM_FATAL("");
    }

    dir1 = dir->OpenDirectory("test");

    if(!dir1)
    {
	NVM_FATAL("");
    }

    if(dir->CreateDirectory("test") != 0)
    {
	NVM_FATAL("");
    }

    if(dir->CreateDirectory("test1") != 0)
    {
	NVM_FATAL("");
    }

    dir1 = dir->OpenDirectory("test");

    if(!dir1)
    {
	NVM_FATAL("");
    }

    dir1 = dir->OpenDirectory("test1");

    if(!dir1)
    {
	NVM_FATAL("");
    }

    nvm_file *open1 = dir->nvm_fopen("test.dir", "w");

    if(!open1)
    {
	NVM_FATAL("");
    }

    dir1 = dir->OpenDirectory("test.dir");

    if(dir1)
    {
	NVM_FATAL("");
    }
}

void TestSubdirectories(nvm_directory *dir, nvm *nvm_api)
{
    nvm_file *fd1;
    nvm_file *fd2;

    if(dir->HasName("root", 4) == false)
    {
	NVM_FATAL("");
    }

    if(dir->HasName("rootdadad", 4) == false)
    {
	NVM_FATAL("");
    }

    if(dir->nvm_fopen("test.c", "r") != nullptr)
    {
	NVM_FATAL("");
    }

    fd1 = dir->nvm_fopen("test.c", "w");

    if(fd1 == nullptr)
    {
	NVM_FATAL("");
    }

    fd2 = dir->nvm_fopen("test.c", "r");

    if(fd2 == nullptr)
    {
	NVM_FATAL("");
    }

    if(fd1 != fd2)
    {
	NVM_FATAL("");
    }

    if(dir->file_look_up("test.c") == nullptr)
    {
	NVM_FATAL("");
    }

    if(dir->file_look_up("test1.c") != nullptr)
    {
	NVM_FATAL("");
    }

    fd1 = dir->nvm_fopen("test/test.c", "w");

    if(fd1 == nullptr)
    {
	NVM_FATAL("");
    }

    fd2 = dir->nvm_fopen("test/test.c", "r");

    if(fd1 == nullptr)
    {
	NVM_FATAL("");
    }

    nvm_directory *dir1 = dir->OpenDirectory("test");

    if(dir1 == nullptr)
    {
	NVM_FATAL("");
    }

    dir1 = dir->OpenDirectory("test/");

    if(dir1 == nullptr)
    {
	NVM_FATAL("");
    }

    fd1 = dir1->nvm_fopen("test.c", "r");

    if(fd1 == nullptr)
    {
	NVM_FATAL("");
    }

    fd2 = dir->nvm_fopen("test", "r");

    if(fd2 != nullptr)
    {
	NVM_FATAL("");
    }

    std::vector<std::string> children;

    if(dir->GetChildren("glgl", &children) == 0)
    {
	NVM_FATAL("");
    }

    if(dir->GetChildren("test", &children) != 0)
    {
	NVM_FATAL("");
    }

    for(unsigned int i = 0; i < children.size(); ++i)
    {
	NVM_DEBUG("%s", children[i].c_str());
    }

    children.clear();
    dir1->GetChildren(&children);

    for(unsigned int i = 0; i < children.size(); ++i)
    {
	NVM_DEBUG("%s", children[i].c_str());
    }

    children.clear();
    dir->GetChildren(&children);

    for(unsigned int i = 0; i < children.size(); ++i)
    {
	NVM_DEBUG("%s", children[i].c_str());
    }

    if(dir->DeleteDirectory("test") != 0)
    {
	NVM_FATAL("");
    }

    dir->CreateDirectory("test14");

    dir->Delete(nvm_api);
}

int main(int argc, char **argv)
{
    nvm_directory *dir;
    nvm *nvm_api;

    ALLOC_CLASS(nvm_api, nvm());
    ALLOC_CLASS(dir, nvm_directory("root", 4, nvm_api, nullptr));

    NVM_DEBUG("init complete");

    if(argc <= 1)
    {
	NVM_DEBUG("NO PARAM");

	goto end;
    }

    switch(argv[1][0])
    {
	case '1':
	{
	    TestOpenAndClose(dir, nvm_api);
	}
	break;

	case '2':
	{
	    TestFileSize(dir, nvm_api);
	}
	break;

	case '3':
	{
	    TestFileDelete(dir, nvm_api);
	}
	break;

	case '4':
	{
	    TestFileModification(dir, nvm_api);
	}
	break;

	case '5':
	{
	    TestFileRename(dir, nvm_api);
	}
	break;

	case '6':
	{
	    TestFileLinkUnlink(dir, nvm_api);
	}
	break;

	case '7':
	{
	    TestFileExists(dir, nvm_api);
	}
	break;

	case '8':
	{
	    TestDirectoryCreate(dir, nvm_api);
	}
	break;

	case '9':
	{
	    TestSubdirectories(dir, nvm_api);
	}
	break;

	default:
	{
	    NVM_DEBUG("UNKNOWN PARAM");
	}
	break;
    }

    delete dir;
    delete nvm_api;

end:

    NVM_DEBUG("TEST FINISHED");

    return 0;
}

#else

int main(void)
{
    return 0;
}

#endif

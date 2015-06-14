#ifdef ROCKSDB_PLATFORM_NVM

#include <iostream>
#include "nvm/nvm.h"

using namespace rocksdb;

void TestWrite(NVMWritableFile *w_file)
{
    char data[5000];

    for(int i = 0; i < 2048; ++i)
    {
	data[i] = 'x';
    }

    Slice s;

    s = Slice(data, 2048);

    w_file->Append(s);

    NVM_DEBUG("Appended first slice");

    for(int i = 2048; i < 4096; ++i)
    {
	data[i] = 'y';
    }

    s = Slice(data + 2048, 2048);

    w_file->Append(s);

    NVM_DEBUG("Appended second slice");

    for(int i = 0; i < 5000; ++i)
    {
	data[i] = 'z';
    }

    s = Slice(data, 5000);

    w_file->Append(s);

    NVM_DEBUG("Appended third slice");
}

void TestSequentialReadAll(NVMSequentialFile *sr_file)
{
    char datax[9096];

    Slice s;

    if(!sr_file->Read(9096, &s, datax).ok())
    {
	NVM_FATAL("");
    }

    size_t len = s.size();
    const char *data = s.data();

    if(len != 9096)
    {
	NVM_FATAL("%lu", len);
    }

    for(int i = 0; i < 2048; ++i)
    {
	if(data[i] != 'x')
	{
	    NVM_FATAL("");
	}
    }

    for(int i = 2048; i < 4096; ++i)
    {
	if(data[i] != 'y')
	{
	    NVM_FATAL("");
	}
    }

    for(int i = 4096; i < 9096; ++i)
    {
	if(data[i] != 'z')
	{
	    NVM_FATAL("");
	}
    }

    sr_file->Read(1, &s, datax);

    len = s.size();

    if(len != 0)
    {
	NVM_FATAL("");
    }
}

void TestSequentialRead(NVMSequentialFile *sr_file)
{
    Slice s;

    char sc[4096];

    if(!sr_file->Read(4096, &s, sc).ok())
    {
	NVM_FATAL("");
    }

    size_t len = s.size();
    const char *data = s.data();

    if(len != 4096)
    {
	NVM_FATAL("%lu", len);
    }

    for(size_t i = 0; i < 2048; ++i)
    {
	if(data[i] != 'x')
	{
	    NVM_FATAL("%lu %c", i, data[i]);
	}
    }

    for(size_t i = 2048; i < 4096; ++i)
    {
	if(data[i] != 'y')
	{
	    NVM_FATAL("%lu %c", i, data[i]);
	}
    }

    size_t cnt = 0;
    while(sr_file->Read(1, &s, sc).ok())
    {
	++cnt;

	len = s.size();

	if(cnt > 5000)
	{
	    if(len > 0)
	    {
		NVM_FATAL("%lu", cnt);
	    }
	    else
	    {
		break;
	    }
	}

	data = s.data();

	if(len != 1)
	{
	    NVM_FATAL("%lu", len);
	}

	if(data[0] != 'z')
	{
	    NVM_FATAL("%lu %c", cnt, data[0]);
	}
    }
}

void TestSequentialSkip(NVMSequentialFile *sr_file)
{
    Slice s;

    char sc;

    if(!sr_file->Read(1, &s, &sc).ok())
    {
	NVM_FATAL("");
    }

    size_t len = s.size();
    const char *data = s.data();

    if(len != 1)
    {
	NVM_FATAL("%lu", len);
    }

    if(data[0] != 'x')
    {
	NVM_FATAL("%c", data[0]);
    }

    if(!sr_file->Skip(2047).ok())
    {
	NVM_FATAL("");
    }

    if(!sr_file->Read(1, &s, &sc).ok())
    {
	NVM_FATAL("");
    }

    len = s.size();
    data = s.data();

    if(len != 1)
    {
	NVM_FATAL("%lu", len);
    }

    if(data[0] != 'y')
    {
	NVM_FATAL("%c", data[0]);
    }

    if(!sr_file->Skip(2047).ok())
    {
	NVM_FATAL("");
    }

    if(!sr_file->Read(1, &s, &sc).ok())
    {
	NVM_FATAL("");
    }

    len = s.size();
    data = s.data();

    if(len != 1)
    {
	NVM_FATAL("%lu", len);
    }

    if(data[0] != 'z')
    {
	NVM_FATAL("%c", data[0]);
    }

    if(!sr_file->Skip(2047).ok())
    {
	NVM_FATAL("");
    }

    if(!sr_file->Read(1, &s, &sc).ok())
    {
	NVM_FATAL("");
    }

    len = s.size();
    data = s.data();

    if(len != 1)
    {
	NVM_FATAL("%lu", len);
    }

    if(data[0] != 'z')
    {
	NVM_FATAL("%c", data[0]);
    }

    if(sr_file->Skip(5000).ok())
    {
	NVM_FATAL("");
    }
}

void TestRandomReads(NVMRandomAccessFile *rr_file)
{
    char datax[10000];

    Slice s;

    if(!rr_file->Read(0, 9096, &s, datax).ok())
    {
	NVM_FATAL("");
    }

    size_t len = s.size();
    const char *data = s.data();

    if(len != 9096)
    {
	NVM_FATAL("%lu", len);
    }

    for(int i = 0; i < 2048; ++i)
    {
	if(data[i] != 'x')
	{
	    NVM_FATAL("");
	}
    }

    for(int i = 2048; i < 4096; ++i)
    {
	if(data[i] != 'y')
	{
	    NVM_FATAL("");
	}
    }

    for(int i = 4096; i < 9096; ++i)
    {
	if(data[i] != 'z')
	{
	    NVM_FATAL("");
	}
    }

    if(!rr_file->Read(0, 1, &s, datax).ok())
    {
	NVM_FATAL("");
    }

    len = s.size();
    data = s.data();

    if(len != 1)
    {
	NVM_FATAL("");
    }

    if(data[0] != 'x')
    {
	NVM_FATAL("");
    }

    if(!rr_file->Read(2048, 1, &s, datax).ok())
    {
	NVM_FATAL("");
    }

    len = s.size();
    data = s.data();

    if(len != 1)
    {
	NVM_FATAL("");
    }

    if(data[0] != 'y')
    {
	NVM_FATAL("");
    }

    if(!rr_file->Read(4096, 1, &s, datax).ok())
    {
	NVM_FATAL("");
    }

    len = s.size();
    data = s.data();

    if(len != 1)
    {
	NVM_FATAL("");
    }

    if(data[0] != 'z')
    {
	NVM_FATAL("");
    }

    if(!rr_file->Read(10000, 1, &s, datax).ok())
    {
	NVM_FATAL("");
    }

    len = s.size();

    if(len != 0)
    {
	NVM_FATAL("");
    }

    if(!rr_file->Read(0, 10000, &s, datax).ok())
    {
	NVM_FATAL("");
    }

    len = s.size();

    if(len != 9096)
    {
	NVM_FATAL("");
    }
}

int main(int argc, char **argv)
{
    nvm_directory *dir;
    nvm *nvm_api;

    ALLOC_CLASS(nvm_api, nvm());
    ALLOC_CLASS(dir, nvm_directory("root", 4, nvm_api));

    nvm_file *wfd = dir->nvm_fopen("test.c", "w");

    if(wfd == nullptr)
    {
	NVM_FATAL("");
    }

    NVM_DEBUG("write file open at %p", wfd);

    nvm_file *srfd = dir->nvm_fopen("test.c", "r");

    if(srfd == nullptr)
    {
	NVM_FATAL("");
    }

    NVM_DEBUG("sequential read file open at %p", srfd);

    NVMSequentialFile *sr_file;
    ALLOC_CLASS(sr_file, NVMSequentialFile("test.c", srfd, dir));

    NVMWritableFile *w_file;
    ALLOC_CLASS(w_file, NVMWritableFile("test.c", wfd, dir));

    TestWrite(w_file);

    delete w_file;

    TestSequentialRead(sr_file);

    delete sr_file;

    ALLOC_CLASS(sr_file, NVMSequentialFile("test.c", srfd, dir));
    TestSequentialSkip(sr_file);

    delete sr_file;

    ALLOC_CLASS(sr_file, NVMSequentialFile("test.c", srfd, dir));
    TestSequentialReadAll(sr_file);

    delete sr_file;

    NVMRandomAccessFile *rr_file;

    ALLOC_CLASS(rr_file, NVMRandomAccessFile("test.c", srfd, dir));
    TestRandomReads(rr_file);

    delete rr_file;

    delete dir;
    delete nvm_api;

    NVM_DEBUG("TEST FINISHED!");

    return 0;
}

#endif

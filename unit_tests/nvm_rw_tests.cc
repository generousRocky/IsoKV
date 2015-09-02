#ifdef ROCKSDB_PLATFORM_NVM

#include <iostream>
#include "nvm/nvm.h"

using namespace rocksdb;

void TestWrite(NVMWritableFile *w_file) {
  char data[5000];

  for(int i = 0; i < 2048; ++i) {
    data[i] = 'x';
  }

  Slice s;

  s = Slice(data, 2048);

  w_file->Append(s);

  NVM_DEBUG("Appended first slice");

  for(int i = 0; i < 2048; ++i) {
    data[i] = 'y';
  }

  s = Slice(data, 2048);

  w_file->Append(s);

  NVM_DEBUG("Appended second slice");
  for(int i = 0; i < 5000; ++i) {
    data[i] = 'z';
  }

  s = Slice(data, 5000);

  w_file->Append(s);

  NVM_DEBUG("Appended third slice");

  w_file->Close();
}

void TestSequentialReadAll(NVMSequentialFile *sr_file) {
  char datax[9096];

  Slice s;

  if(!sr_file->Read(9096, &s, datax).ok()) {
    NVM_FATAL("");
  }

  size_t len = s.size();
  const char *data = s.data();

  if(len != 9096) {
    NVM_FATAL("%lu", len);
  }

  for(int i = 0; i < 2048; ++i) {
    if(data[i] != 'x') {
      NVM_FATAL("");
    }
  }

  for(int i = 2048; i < 4096; ++i) {
    if(data[i] != 'y') {
      NVM_FATAL("");
    }
  }

  for(int i = 4096; i < 9096; ++i) {
    if(data[i] != 'z') {
      NVM_FATAL("");
    }
  }

  sr_file->Read(1, &s, datax);

  len = s.size();

  // if(len != 0) {
    // NVM_FATAL("");
  // }
}

void TestSequentialRead(NVMSequentialFile *sr_file) {
  Slice s;

  char sc[4096];

  if(!sr_file->Read(4096, &s, sc).ok()) {
    NVM_FATAL("");
  }

  size_t len = s.size();
  const char *data = s.data();

  if(len != 4096) {
    NVM_FATAL("%lu", len);
  }

  for(size_t i = 0; i < 2048; ++i) {
    // NVM_DEBUG("data[%lu]: %c\n", i, data[i]);
    if(data[i] != 'x') {
      NVM_FATAL("%lu %c", i, data[i]);
    }
  }

  // NVM_DEBUG("YAY!!!!\n");

  for(size_t i = 2048; i < 4096; ++i) {
    // NVM_DEBUG("data[%lu]: %c\n", i, data[i]);
    if(data[i] != 'y') {
      NVM_FATAL("%lu %c", i, data[i]);
    }
  }
  // NVM_DEBUG("YAY!!!!\n");

  size_t cnt = 0;
  while(sr_file->Read(1, &s, sc).ok()) {
    ++cnt;

    len = s.size();

    if(cnt > 5000) {
      if(len > 0) {
        NVM_FATAL("%lu, len:%lu", cnt, len);
      } else {
        break;
      }
    }

    data = s.data();

    if(len != 1) {
      NVM_FATAL("%lu", len);
    }

    if(data[0] != 'z') {
      NVM_FATAL("%lu %c", cnt, data[0]);
    }
  }
}

void TestSequentialSkip(NVMSequentialFile *sr_file) {
  Slice s;

  char sc;

  if(!sr_file->Read(1, &s, &sc).ok()) {
    NVM_FATAL("");
  }

  size_t len = s.size();
  const char *data = s.data();

  if(len != 1) {
    NVM_FATAL("%lu", len);
  }

  if(data[0] != 'x') {
    NVM_FATAL("%c", data[0]);
  }

  if(!sr_file->Skip(2047).ok()) {
    NVM_FATAL("");
  }

  if(!sr_file->Read(1, &s, &sc).ok()) {
    NVM_FATAL("");
  }

  len = s.size();
  data = s.data();

  if(len != 1) {
    NVM_FATAL("%lu", len);
  }

  if(data[0] != 'y') {
    NVM_FATAL("%c", data[0]);
  }

  if(!sr_file->Skip(2047).ok()) {
    NVM_FATAL("");
  }

  if(!sr_file->Read(1, &s, &sc).ok()) {
    NVM_FATAL("");
  }

  len = s.size();
  data = s.data();

  if(len != 1) {
    NVM_FATAL("%lu", len);
  }

  if(data[0] != 'z') {
    NVM_FATAL("%c", data[0]);
  }

  if(!sr_file->Skip(2047).ok()) {
    NVM_FATAL("");
  }

  if(!sr_file->Read(1, &s, &sc).ok()) {
    NVM_FATAL("");
  }

  len = s.size();
  data = s.data();

  if(len != 1) {
    NVM_FATAL("%lu", len);
  }

  if(data[0] != 'z') {
    NVM_FATAL("%c", data[0]);
  }

  if(sr_file->Skip(5000).ok()) {
    NVM_FATAL("");
  }
}

void TestRandomReads(NVMRandomAccessFile *rr_file) {
  char datax[10000];

  Slice s;

  if(!rr_file->Read(0, 9096, &s, datax).ok()) {
    NVM_FATAL("");
  }

  size_t len = s.size();
  const char *data = s.data();

  if(len != 9096) {
    NVM_FATAL("%lu", len);
  }

  for(int i = 0; i < 2048; ++i) {
    if(data[i] != 'x') {
      NVM_FATAL("");
    }
  }

  for(int i = 2048; i < 4096; ++i) {
    if(data[i] != 'y') {
      NVM_FATAL("");
    }
  }

  for(int i = 4096; i < 9096; ++i) {
    if(data[i] != 'z') {
      NVM_FATAL("");
    }
  }

  if(!rr_file->Read(0, 1, &s, datax).ok()) {
    NVM_FATAL("");
  }

  len = s.size();
  data = s.data();

  if(len != 1) {
    NVM_FATAL("");
  }

  if(data[0] != 'x') {
    NVM_FATAL("");
  }

  if(!rr_file->Read(2048, 1, &s, datax).ok()) {
    NVM_FATAL("");
  }

  len = s.size();
  data = s.data();

  if(len != 1) {
    NVM_FATAL("");
  }

  if(data[0] != 'y') {
    NVM_FATAL("");
  }

  if(!rr_file->Read(4096, 1, &s, datax).ok()) {
    NVM_FATAL("");
  }

  len = s.size();
  data = s.data();

  if(len != 1) {
    NVM_FATAL("");
  }

  if(data[0] != 'z') {
    NVM_FATAL("");
  }

  if(!rr_file->Read(10000, 1, &s, datax).ok()) {
    NVM_FATAL("");
  }

  len = s.size();

  if(len != 0) {
    NVM_FATAL("");
  }

  if(!rr_file->Read(0, 10000, &s, datax).ok()) {
    NVM_FATAL("");
  }

  len = s.size();

  if(len != 9096) {
    NVM_FATAL("");
  }
}

void TestRandomReadWrites(NVMRandomRWFile *rw_file) {
  char datax[10000];

  Slice s;

  if(!rw_file->Read(0, 9096, &s, datax).ok()) {
    NVM_FATAL("");
  }

  size_t len = s.size();
  const char *data = s.data();

  if(len != 9096) {
    NVM_FATAL("%lu", len);
  }

  for(int i = 0; i < 2048; ++i) {
    if(data[i] != 'x') {
      NVM_FATAL("");
    }
  }

  for(int i = 2048; i < 4096; ++i) {
    if(data[i] != 'y') {
      NVM_FATAL("");
    }
  }

  for(int i = 4096; i < 9096; ++i) {
    if(data[i] != 'z') {
      NVM_FATAL("");
    }
  }

  if(!rw_file->Read(0, 1, &s, datax).ok()) {
    NVM_FATAL("");
  }

  len = s.size();
  data = s.data();

  if(len != 1) {
    NVM_FATAL("");
  }

  if(data[0] != 'x') {
    NVM_FATAL("");
  }

  if(!rw_file->Read(2048, 1, &s, datax).ok()) {
    NVM_FATAL("");
  }

  len = s.size();
  data = s.data();

  if(len != 1) {
    NVM_FATAL("");
  }

  if(data[0] != 'y') {
    NVM_FATAL("");
  }

  if(!rw_file->Read(4096, 1, &s, datax).ok()) {
    NVM_FATAL("");
  }

  len = s.size();
  data = s.data();

  if(len != 1) {
    NVM_FATAL("");
  }

  if(data[0] != 'z') {
    NVM_FATAL("");
  }

  if(!rw_file->Read(10000, 1, &s, datax).ok()) {
    NVM_FATAL("");
  }

  len = s.size();

  if(len != 0) {
    NVM_FATAL("");
  }

  if(!rw_file->Read(0, 10000, &s, datax).ok()) {
    NVM_FATAL("");
  }

  len = s.size();

  if(len != 9096) {
    NVM_FATAL("");
  }

  for(int i = 0; i < 2048; ++i) {
    datax[i] = 'm';
  }

  s = Slice(datax, 2048);

  if(!rw_file->Write(0, s).ok()) {
    NVM_FATAL("");
  }

  if(!rw_file->Read(0, 4096, &s, datax).ok()) {
    NVM_FATAL("");
  }

  len = s.size();
  data = s.data();

  if(len != 4096) {
    NVM_FATAL("");
  }

  for(int i = 0; i < 2048; ++i) {
    if(data[i] != 'm') {
      NVM_FATAL("");
    }
  }

  for(int i = 2048; i < 4096; ++i) {
    if(data[i] != 'y') {
      NVM_FATAL("%c", data[i]);
    }
  }

  for(int i = 0; i < 5000; ++i) {
    datax[i] = 'o';
  }

  s = Slice(datax, 5000);

  if(!rw_file->Write(2059, s).ok()) {
    NVM_FATAL("");
  }

  if(!rw_file->Read(2059, 5001, &s, datax).ok()) {
    NVM_FATAL("");
  }

  len = s.size();
  data = s.data();

  if(len != 5001) {
    NVM_FATAL("");
  }

  for(int i = 0; i < 5000; ++i) {
    if(data[i] != 'o') {
      NVM_FATAL("");
    }
  }

  if(data[5000] != 'z') {
    NVM_FATAL("%c", data[5000]);
  }
}

void test1() {
  nvm_directory *dir;
  nvm *nvm_api;

  ALLOC_CLASS(nvm_api, nvm());
  ALLOC_CLASS(dir, nvm_directory("root", 4, nvm_api, nullptr));

  nvm_file *wfd = dir->nvm_fopen("test.c", "w");

  if(wfd == nullptr) {
    NVM_FATAL("");
  }

  NVM_DEBUG("write file open at %p", wfd);

  nvm_file *srfd = dir->nvm_fopen("test.c", "r");

  if(srfd == nullptr) {
    NVM_FATAL("");
  }

  NVM_DEBUG("sequential read file open at %p", srfd);


  NVMWritableFile *w_file;
  ALLOC_CLASS(w_file, NVMWritableFile("test.c", wfd, dir));

  TestWrite(w_file);

  //When deleting it closes the file and flushes data to disk
  delete(w_file);

  NVMSequentialFile *sr_file;

  ALLOC_CLASS(sr_file, NVMSequentialFile("test.c", srfd, dir));
  TestSequentialRead(sr_file);
  delete sr_file;

  ALLOC_CLASS(sr_file, NVMSequentialFile("test.c", srfd, dir));
  TestSequentialSkip(sr_file);
  delete sr_file;

  ALLOC_CLASS(sr_file, NVMSequentialFile("test.c", srfd, dir));
  TestSequentialReadAll(sr_file);

  delete sr_file;

  // NVMRandomAccessFile *rr_file;

  // ALLOC_CLASS(rr_file, NVMRandomAccessFile("test.c", srfd, dir));
  // TestRandomReads(rr_file);

  // delete rr_file;
#if 0
  NVMRandomRWFile *rw_file;

  ALLOC_CLASS(rw_file, NVMRandomRWFile("test.c", wfd, dir));
  TestRandomReadWrites(rw_file);

  delete rw_file;
#endif

  delete dir;
  delete nvm_api;

  NVM_DEBUG("TEST FINISHED!");
}

void test2() {
  nvm_directory *dir;
  nvm *nvm_api;

  ALLOC_CLASS(nvm_api, nvm());
  ALLOC_CLASS(dir, nvm_directory("root", 4, nvm_api, nullptr));

  nvm_file *wfd = dir->nvm_fopen("test.c", "w");

  if(wfd == nullptr) {
    NVM_FATAL("");
  }

  NVMWritableFile *w_file;
  NVMRandomAccessFile *ra_file;

  char data[100];

  Slice s;

  for(int i = 0; i < 100; ++i) {
    for(int j = 0; j < 100; ++j) {
      data[j] = i;
    }

    s = Slice(data, 100);

    ALLOC_CLASS(w_file, NVMWritableFile("test.c", wfd, dir));

    w_file->Append(s);
    w_file->Close();

    delete w_file;

    unsigned long actual_size = wfd->GetSize();
    unsigned long expected_size = 100 * (i + 1);

    if(actual_size != expected_size) {
      NVM_FATAL("%lu vs %lu", actual_size, expected_size);
    }
  }

  ALLOC_CLASS(ra_file, NVMRandomAccessFile("test.c", wfd, dir));

  for(int i = 0; i < 100; ++i) {
    ra_file->Read(i * 100, 100, &s, data);

    const char *sd = s.data();
    unsigned long len = s.size();

    for(unsigned long j = 0; j < len; ++j) {
      if(sd[j] != i) {
        NVM_FATAL("");
      }
    }
  }

  delete ra_file;

  NVM_DEBUG("TEST FINISHED!");
}


int main(int argc, char **argv) {
  test1();
  // test2();

  return 0;
}

#else

int main(void) {
  return 0;
}

#endif

#ifdef ROCKSDB_PLATFORM_NVM

#include <iostream>
#include "nvm/nvm.h"

using namespace rocksdb;

void w_test_1() {
  nvm_directory *dir;
  nvm *nvm_api;

  ALLOC_CLASS(nvm_api, nvm());
  ALLOC_CLASS(dir, nvm_directory("root", 4, nvm_api, nullptr));

  nvm_file *wfd = dir->nvm_fopen("test.c", "w");
  if(wfd == nullptr) {
    NVM_FATAL("");
  }

  nvm_file *srfd = dir->nvm_fopen("test.c", "r");
  if(srfd == nullptr) {
    NVM_FATAL("");
  }

  NVMWritableFile *w_file;
  NVMSequentialFile *sr_file;

  char data[100];
  char datax[100];

  Slice s, t;

  for (int i = 0; i < 100; ++i) {
    data[i] = i;
  }

  s = Slice(data, 100);

  ALLOC_CLASS(w_file, NVMWritableFile("test.c", wfd, dir));
  w_file->Append(s);

  w_file->Close();

  // Test SequentialFile::Read
  ALLOC_CLASS(sr_file, NVMSequentialFile("test2.c", srfd, dir));
  if (!sr_file->Read(100, &t, datax).ok()) {
    NVM_FATAL("");
  }

  size_t len = t.size();
  const char *data_read = t.data();

  if (len != 100) {
    NVM_FATAL("%lu", len);
  }

  for (int i = 0; i < 100; i++) {
    if (data_read[i] != data[i]) {
      NVM_FATAL("");
    }
  }

  delete sr_file;

  // Test SequentialFile::Skip
  ALLOC_CLASS(sr_file, NVMSequentialFile("test2.c", srfd, dir));
  if (!sr_file->Read(10, &t, datax).ok()) {
    NVM_FATAL("");
  }

  len = t.size();
  data_read = t.data();

  if (len != 10) {
    NVM_FATAL("%lu", len);
  }

  for (int i = 0; i < 10; i++) {
    if (data_read[i] != data[i]) {
      NVM_FATAL("");
    }
  }

  if (!sr_file->Skip(10).ok()) {
    NVM_FATAL("");
  }

  if (!sr_file->Read(80, &t, datax).ok()) {
    NVM_FATAL("");
  }

  len = t.size();
  data_read = t.data();

  if (len != 80) {
    NVM_FATAL("%lu", len);
  }

  for (int i = 0; i < 80; i++) {
    if (data_read[i] != data[i + 20]) {
      NVM_FATAL("");
    }
  }

  delete sr_file;

  //Test RandomAccessFile::Read
  NVMRandomAccessFile *rr_file;
  ALLOC_CLASS(rr_file, NVMRandomAccessFile("test2.c", srfd, dir));


  for (int i = 0; i < 100; i++) {
    if (!rr_file->Read(i, 1, &t, datax).ok()) {
      NVM_FATAL("");
    }

    len = t.size();
    data_read = t.data();

    if (len != 1) {
      NVM_FATAL("%lu", len);
    }

    if (data_read[0] != data[i]) {
      NVM_FATAL("");
    }
  }

  delete rr_file;
  delete w_file;

  NVM_DEBUG("TEST FINISHED!");
}

void w_block_test_1() {
  nvm_directory *dir;
  nvm *nvm_api;

  ALLOC_CLASS(nvm_api, nvm());
  ALLOC_CLASS(dir, nvm_directory("root", 4, nvm_api, nullptr));

  nvm_file *wfd = dir->nvm_fopen("test.c", "w");
  if(wfd == nullptr) {
    NVM_FATAL("");
  }

  NVMWritableFile *w_file;

  char data[128 * 4096];
  memset(data, 'a', 128 * 4096);

  Slice s;

  s = Slice(data, 128 * 4096);
  ALLOC_CLASS(w_file, NVMWritableFile("test.c", wfd, dir));
  w_file->Append(s);
  w_file->Close();


  delete w_file;
  NVM_DEBUG("TEST FINISHED!");
}

int main(int argc, char **argv) {
  w_test_1();
  // w_block_test_1();

  return 0;
}

#else // ROCKSDB_PLATFORM_NVM

int main(void) {
  return 0;
}

#endif // ROCKSDB_PLATFORM_NVM

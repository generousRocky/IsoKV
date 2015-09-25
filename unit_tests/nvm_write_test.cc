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

  nvm_file *srfd = dir->nvm_fopen("test.c", "r");
  if(srfd == nullptr) {
    NVM_FATAL("");
  }

  NVMWritableFile *w_file;
  NVMSequentialFile *sr_file;

  char data[10 * 4096];
  char datax[10 * 4096];

  for (long i = 0; i < 10 * 4096; i++) {
    data[i] = i % 4096;
  }

  Slice s, t;

  s = Slice(data, 10 * 4096);
  ALLOC_CLASS(w_file, NVMWritableFile("test.c", wfd, dir));
  w_file->Append(s);
  w_file->Close();

  ALLOC_CLASS(sr_file, NVMSequentialFile("test2.c", srfd, dir));
  if (!sr_file->Read(10 * 4096, &t, datax).ok()) {
    NVM_FATAL("");
  }

  size_t len = t.size();
  const char *data_read = t.data();

  if (len != 10 * 4096) {
    NVM_FATAL("%lu", len);
  }

  for (long i = 0; i < 10 * 4096; i++) {
    if (data_read[i] != data[i]) {
      NVM_FATAL("i: %lu\n", i);
    }
  }

  delete sr_file;
  delete w_file;

  NVM_DEBUG("TEST 1 FINISHED!");
}

void w_block_test_2() {
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

  char data[5 * 4096];
  char datax[5 * 4096];

  ALLOC_CLASS(w_file, NVMWritableFile("test.c", wfd, dir));
  Slice s, t;

  char input = 'a';
  for (int i = 0; i < 5; i++) {
    for (int j = 0; j < 4096; j++) {
      data[(i * 4096) + j] = input + i;
    }

    s = Slice(data + (i * 4096), 4096);
    w_file->Append(s);
  }
  w_file->Close();

  ALLOC_CLASS(sr_file, NVMSequentialFile("test2.c", srfd, dir));
  if (!sr_file->Read(5 * 4096, &t, datax).ok()) {
    NVM_FATAL("");
  }

  size_t len = t.size();
  const char *data_read = t.data();

  if (len != 5 * 4096) {
    NVM_FATAL("%lu", len);
  }

  for (long i = 0; i < 5 * 4096; i++) {
    if (data_read[i] != data[i]) {
      NVM_FATAL("");
    }
  }

  delete sr_file;
  delete w_file;

  NVM_DEBUG("TEST 2 FINISHED!");
}

void w_block_test_3() {
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

  char data[5 * 4096];
  char datax[5 * 4096];

  ALLOC_CLASS(w_file, NVMWritableFile("test.c", wfd, dir));
  Slice s, t;

  char input = 'a';
  for (int i = 0; i < 10; i++) {
    for (int j = 0; j < 2048; j++) {
      data[(i * 2048) + j] = input + i;
    }

    s = Slice(data + (i * 2048), 2048);
    w_file->Append(s);
  }
  w_file->Close();

  ALLOC_CLASS(sr_file, NVMSequentialFile("test2.c", srfd, dir));
  if (!sr_file->Read(5 * 4096, &t, datax).ok()) {
    NVM_FATAL("");
  }

  size_t len = t.size();
  const char *data_read = t.data();

  if (len != 5 * 4096) {
    NVM_FATAL("%lu", len);
  }

  for (long i = 0; i < 5 * 4096; i++) {
    if (data_read[i] != data[i]) {
      NVM_FATAL("");
    }
  }

  NVMRandomAccessFile *rr_file;
  ALLOC_CLASS(rr_file, NVMRandomAccessFile("test2.c", srfd, dir));
  for (size_t i = 0; i < 5 * 4096; i++) {
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

  if (!rr_file->Read(0, 5 * 4096, &t, datax).ok()) {
     NVM_FATAL("");
   }

   len = t.size();
   data_read = t.data();

   if (len != 5 * 4096) {
     NVM_FATAL("%lu", len);
   }

  for (size_t i = 0; i < 5 * 4096; i++) {
    if (data_read[i] != data[i]) {
      NVM_FATAL("");
    }
  }

  if (!rr_file->Read(500, (5 * 4096) - 500, &t, datax).ok()) {
     NVM_FATAL("");
   }

   len = t.size();
   data_read = t.data();

   if (len != (5 * 4096) - 500) {
     NVM_FATAL("%lu", len);
   }

  for (size_t i = 0; i < (5 * 4096) - 500; i++) {
    if (data_read[i] != data[500 + i]) {
      NVM_FATAL("");
    }
  }

  if (!rr_file->Read(5000, (5 * 4096) - 5000, &t, datax).ok()) {
     NVM_FATAL("");
   }

   len = t.size();
   data_read = t.data();

   if (len != (5 * 4096) - 5000) {
     NVM_FATAL("%lu", len);
   }

  for (size_t i = 0; i < (5 * 4096) - 5000; i++) {
    if (data_read[i] != data[5000 + i]) {
      NVM_FATAL("");
    }
  }

  delete sr_file;
  delete rr_file;
  delete w_file;
  delete dir;
  delete nvm_api;

  NVM_DEBUG("TEST 3 FINISHED!");
}

void w_block_test_4() {
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

  //Write enough data to trigger a new block creation
  char data[200 * 4096];
  char datax[200 * 4096];

  ALLOC_CLASS(w_file, NVMWritableFile("test.c", wfd, dir));
  Slice s, t;

  char input = 'a';
  for (int i = 0; i < 400; i++) {
    for (int j = 0; j < 2048; j++) {
      data[(i * 2048) + j] = (input + i);
    }

    s = Slice(data + (i * 2048), 2048);
    w_file->Append(s);
  }
  w_file->Close();

  ALLOC_CLASS(sr_file, NVMSequentialFile("test2.c", srfd, dir));
  if (!sr_file->Read(200 * 4096, &t, datax).ok()) {
    NVM_FATAL("");
  }

  size_t len = t.size();
  const char *data_read = t.data();

  if (len != 200 * 4096) {
    NVM_FATAL("%lu", len);
  }

  for (long i = 0; i < 200 * 4096; i++) {
    if (data_read[i] != data[i]) {
      NVM_FATAL("");
    }
  }
  delete sr_file;

  ALLOC_CLASS(sr_file, NVMSequentialFile("test2.c", srfd, dir));
  if (!sr_file->Read(128 * 4096, &t, datax).ok()) {
    NVM_FATAL("");
  }

  len = t.size();
  data_read = t.data();

  if (len != 128 * 4096) {
    NVM_FATAL("%lu", len);
  }

  for (long i = 0; i < 128 * 4096; i++) {
    if (data_read[i] != data[i]) {
      NVM_FATAL("");
    }
  }

  if (!sr_file->Read(8 * 4096, &t, datax).ok()) {
    NVM_FATAL("");
  }

  len = t.size();
  data_read = t.data();

  if (len != 8 * 4096) {
    NVM_FATAL("%lu", len);
  }

  for (long i = 0; i < 8 * 4096; i++) {
    if (data_read[i] != data[(128 * 4096) + i]) {
      NVM_FATAL("");
    }
  }


  NVMRandomAccessFile *rr_file;
  ALLOC_CLASS(rr_file, NVMRandomAccessFile("test2.c", srfd, dir));

  // Read randomly from a block that is not initial block
  if (!rr_file->Read(130 * 4096, 10, &t, datax).ok()) {
     NVM_FATAL("");
  }

  len = t.size();
  data_read = t.data();

  if (len != 10) {
    NVM_FATAL("%lu", len);
  }

  for (size_t i = 0; i < 10; i++) {
    if (data_read[i] != data[(130 * 4096) + i]) {
      NVM_FATAL("%lu", i);
    }
  }

  delete sr_file;
  delete rr_file;
  delete w_file;
  delete dir;
  delete nvm_api;

  NVM_DEBUG("TEST 4 FINISHED!");
}

void w_block_test_5() {
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

  //Write enough data to trigger a new block creation
  char data[200 * 4096];
  char datax[200 * 4096];

  ALLOC_CLASS(w_file, NVMWritableFile("test.c", wfd, dir));
  Slice s, t;

  //write in block disaligned chunks
  char input = 'a';
  for (int i = 0; i < 300; i++) {
    for (int j = 0; j < 2060; j++) {
      data[(i * 2060) + j] = (input + i);
    }

    s = Slice(data + (i * 2060), 2060);
    w_file->Append(s);
  }
  w_file->Close();

  ALLOC_CLASS(sr_file, NVMSequentialFile("test2.c", srfd, dir));
  if (!sr_file->Read(200 * 4096, &t, datax).ok()) {
    NVM_FATAL("");
  }

  size_t len = t.size();
  const char *data_read = t.data();

  if (len != 300 * 2060) {
    NVM_FATAL("%lu", len);
  }

  for (long i = 0; i < 300 * 2060; i++) {
    if (data_read[i] != data[i]) {
      NVM_FATAL("");
    }
  }

  delete sr_file;
  delete w_file;
  delete dir;
  delete nvm_api;

  NVM_DEBUG("TEST 5 FINISHED!");
}

void w_block_test_6() {
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

  //Write enough data to trigger a new block creation
  char data[400 * 4096];
  char datax[400 * 4096];

  ALLOC_CLASS(w_file, NVMWritableFile("test.c", wfd, dir));
  Slice s, t;

  //write in block disaligned chunks
  char input = 'a';
  for (int i = 0; i < 400; i++) {
    for (int j = 0; j < 4096; j++) {
      data[(i * 4096) + j] = (input + i);
    }
  }

  int j = 380;
  for (size_t i = 0; i < 400 * 4096; ) {
    s = Slice(data + (i + 380), 380);
    w_file->Append(s);
    i += j;
  }

  w_file->Close();

  ALLOC_CLASS(sr_file, NVMSequentialFile("test2.c", srfd, dir));
  for (size_t i = 0; i < 300 * 4096; ) {
    if (!sr_file->Read(400, &t, datax).ok()) {
      NVM_FATAL("");
    }

    size_t len = t.size();
    const char *data_read = t.data();

    if (len !=  400) {
      NVM_FATAL("%lu", len);
     }

    for (int k = 0; k < 400; k++) {
      if (data[i + k] != data_read[k]) {
        NVM_FATAL("");
      }
    }

    i += 400;
  }

  delete sr_file;
  delete w_file;
  delete dir;
  delete nvm_api;

  NVM_DEBUG("TEST 6 FINISHED!");
}


int main(int argc, char **argv) {
  // w_test_1();
  // w_block_test_1();
  // w_block_test_2();
  // w_block_test_3();
  // w_block_test_4();
  // w_block_test_5();
  w_block_test_6();

  return 0;
}

#else // ROCKSDB_PLATFORM_NVM

int main(void) {
  return 0;
}

#endif // ROCKSDB_PLATFORM_NVM

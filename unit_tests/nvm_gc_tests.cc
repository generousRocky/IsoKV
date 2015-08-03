#ifdef ROCKSDB_PLATFORM_NVM

#include "nvm/nvm.h"

using namespace rocksdb;

int main(int argc, char **argv) {
  nvm_directory *dir;
  nvm *nvm_api;

  ALLOC_CLASS(nvm_api, nvm());
  ALLOC_CLASS(dir, nvm_directory("root", 4, nvm_api, nullptr));

  nvm_file *fd = dir->nvm_fopen("test.c", "w");

  if (fd == nullptr) {
    NVM_FATAL("");
  }

  NVMRandomRWFile *rw_file;

  ALLOC_CLASS(rw_file, NVMRandomRWFile("test.c", fd, dir));

  char data[4096];

  for (int i = 0; i < 100; ++i) {
    data[i] = '9';
  }

  Slice s = Slice(data, 100);

  if (!rw_file->Write(0, s).ok()) {
    NVM_FATAL("");
  }

  if (!rw_file->Read(0, 4096, &s, data).ok()) {
    NVM_FATAL("");
  }

  if (s.size() != 100) {
    NVM_FATAL("%lu", s.size());
  }

  const char *read_data;

  read_data = s.data();

  for (unsigned int i = 0; i < s.size(); ++i) {
    if (read_data[i] != '9') {
      NVM_FATAL("");
    }
  }

  for (unsigned int i = 0; i < 100; ++i) {
    data[i] = '8';
  }

  s = Slice(data, 100);

  if (!rw_file->Write(0, s).ok()) {
    NVM_FATAL("");
  }

  if (!rw_file->Read(0, 4096, &s, data).ok()) {
    NVM_FATAL("");
  }

  read_data = s.data();

  if (s.size() != 100) {
    NVM_FATAL("");
  }

  for (unsigned int i = 0; i < s.size(); ++i) {
    if (read_data[i] != '8') {
      NVM_FATAL("");
    }
  }

  nvm_api->GarbageCollection();

  if (!rw_file->Read(0, 4096, &s, data).ok()) {
    NVM_FATAL("");
  }

  if (s.size() != 100) {
    NVM_FATAL("");
  }

  read_data = s.data();

  for (unsigned int i = 0; i < s.size(); ++i) {
    if (read_data[i] != '8') {
      NVM_FATAL("");
    }
  }

  NVM_DEBUG("TEST FINISHED");

  return 0;
}

#else

int main(void) {
  return 0;
}

#endif

#ifdef ROCKSDB_PLATFORM_NVM

#include "nvm/nvm.h"

using namespace rocksdb;

void TestFtlSave(nvm *nvm_api, nvm_directory *dir) {
  dir->CreateDirectory("test");
  dir->CreateDirectory("test1");
  dir->CreateDirectory("test2");

  nvm_directory *dir1 = dir->OpenDirectory("test1");

  dir1->CreateDirectory("testx");
  dir1->CreateDirectory("testxx");
  dir1->CreateDirectory("testxxx");

  dir1->nvm_fopen("ftestx", "w");
  dir1->nvm_fopen("ftestxx", "w");

  int fd = open("root_nvm.layout", O_RDWR | O_CREAT, S_IWUSR | S_IRUSR);

  if (fd < 0) {
    NVM_FATAL("");
  }

  if (!dir->Save(fd).ok()) {
    NVM_FATAL("");
  }

  close(fd);
}

void TestFtlLoad(nvm *nvm_api, nvm_directory *dir) {
  char temp;

  int fd = open("root_nvm.layout", O_RDONLY);

  if (read(fd, &temp, 1) != 1) {
    NVM_FATAL("");
  }

  if (temp != 'd') {
    NVM_FATAL("");
  }

  dir->Load(fd);

  close(fd);

  fd = open("root_nvm.layout2", O_RDWR | O_CREAT, S_IWUSR | S_IRUSR);
  if (fd < 0) {
    NVM_FATAL("");
  }

  if (!dir->Save(fd).ok()) {
    NVM_FATAL("");
  }

  close(fd);
}

int main(int argc, char **argv) {
  nvm_directory *save_dir;
  nvm_directory *load_dir;
  nvm *nvm_api;

  ALLOC_CLASS(nvm_api, nvm());
  ALLOC_CLASS(save_dir, nvm_directory("root", 4, nvm_api, nullptr));
  ALLOC_CLASS(load_dir, nvm_directory("root", 4, nvm_api, nullptr));

  TestFtlSave(nvm_api, save_dir);

  NVM_DEBUG("\nLOADING\n")

  TestFtlLoad(nvm_api, load_dir);

  return 0;
}

#else

int main(void) {
  return 0;
}

#endif

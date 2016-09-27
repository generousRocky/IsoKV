#include <iostream>
#include <sstream>
#include <cassert>

#include "rocksdb/db.h"
#include "rocksdb/utilities/env_registry.h"
#include "env_nvm.h"
#include "args.h"

using namespace rocksdb;

static size_t kMB = 1 << 20;
static char fn[] = "/tmp/testfile.rdb";
//static char fn[] = "/tmp/testfile.log";

int main(int argc, char *argv[]) {

  Args args(argc, argv, {"foo"});
  if (argc < 2) {
    args.pr_usage();
    return -1;
  }
  args.pr_args();

  std::unique_ptr<Env> env_guard;
  Env *env = NewEnvFromUri("nvm://nvme0n1", &env_guard);
  assert(env);

  EnvOptions opt_e;
  Status s;

  unique_ptr<WritableFile> wfile;
  s = env->NewWritableFile(fn, &wfile, opt_e);
  assert(s.ok());

  size_t align = wfile->GetRequiredBufferAlignment();

  char *scratch = new char[kMB];
  char *rbuf = new char[kMB];
  char *wbuf = new char[kMB];

  for (size_t i = 0; i < kMB; ++i) {
    wbuf[i] = (i % 26) + 65;
  }
  wbuf[kMB-1] = '\n';

  for (size_t offset = 0; offset < kMB; offset += align) {
    Slice wslice(wbuf+offset, align);
    s = wfile->Append(wslice);
    assert(s.ok());
  }

  wfile.reset(nullptr);

  unique_ptr<SequentialFile> rfile;
  s = env->NewSequentialFile(fn, &rfile, opt_e);
  assert(s.ok());

  Slice rslice(rbuf, kMB);
  s = rfile->Read(kMB, &rslice, scratch);
  assert(s.ok());

  rfile.reset(nullptr);

  size_t nerr = 0;
  for (size_t i = 0; (i < kMB) && (nerr < 10); ++i) {
    if (wbuf[i] != rslice[i]) {
      ++nerr;
      std::cout << "wbuf[" << i << "](" << wbuf[i] << ") != "
                << "rbuf[" << i << "](" << rslice[i] << ")" << std::endl;
    }
    assert(wbuf[i] == rslice[i]);
  }

  delete [] wbuf;
  delete [] rbuf;
  delete [] scratch;

  return 0;
}


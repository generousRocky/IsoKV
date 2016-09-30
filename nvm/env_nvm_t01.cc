#include <iostream>
#include <sstream>
#include <cassert>

#include "rocksdb/db.h"
#include "rocksdb/utilities/env_registry.h"
#include "env_nvm.h"
#include "args.h"

using namespace rocksdb;

static size_t kMB = 1 << 20;
static size_t kFsize = kMB * 16;
static char fn[] = "/tmp/testfile.log";

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

  char *scratch = new char[kFsize];
  char *rbuf = new char[kFsize];
  char *wbuf = new char[kFsize];

  char prefix[] = "++++START++++";
  char suffix[] = "----END----\n";

  strcpy(wbuf, prefix);
  for (size_t i = strlen(prefix); i < kFsize; ++i) {
    //wbuf[i] = (i % 26) + 65;
    wbuf[i] = 65;
  }
  strcpy(wbuf + (kFsize - strlen(suffix)), suffix);

  for (size_t offset = 0; offset < kFsize; offset += align) {
    Slice wslice(wbuf+offset, align);
    s = wfile->Append(wslice);
    assert(s.ok());

    if (!(offset % (align * 4)))  // Flush after every fourth append
      s = wfile->Flush();
  }

  wfile.reset(nullptr);

  unique_ptr<SequentialFile> rfile;
  s = env->NewSequentialFile(fn, &rfile, opt_e);
  assert(s.ok());

  Slice rslice(rbuf, kFsize);
  s = rfile->Read(kFsize, &rslice, scratch);
  assert(s.ok());

  rfile.reset(nullptr);

  size_t nerr = 0;
  for (size_t i = 0; i < kFsize; ++i) {
    assert(wbuf[i] == rslice[i]);
    if ((wbuf[i] != rslice[i]) && \
      ((i < 50) || (i > kFsize-50) || (rslice[i] > 65 || rslice[i] < 90))) {
      ++nerr;
      std::cout << "wbuf[" << i << "](" << wbuf[i] << ") != "
                << "rbuf[" << i << "](" << rslice[i] << ")" << std::endl;
    }
  }

  delete [] wbuf;
  delete [] rbuf;
  delete [] scratch;

  return 0;
}


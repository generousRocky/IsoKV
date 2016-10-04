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

#include "env_nvm_t00.cc"

int main(int argc, char *argv[]) {

  Args args(argc, argv, {"delete_existing"});
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

  Sample sample(kFsize);                // Create buffers
  sample.fill(65);

  s = env->FileExists(fn);
  if (s.ok() && args["delete_existing"]) {
    env->DeleteFile(fn);
  }

  s = env->FileExists(fn);
  if (!s.ok()) {                        // Construct the file
    unique_ptr<WritableFile> wfile;
    s = env->NewWritableFile(fn, &wfile, opt_e);
    assert(s.ok());

    size_t align = wfile->GetRequiredBufferAlignment();

    for (size_t offset = 0; offset < kFsize; offset += align) {
      Slice wslice(sample.wbuf + offset, align);
      s = wfile->Append(wslice);
      assert(s.ok());

      if (!(offset % (align * 4)))      // Flush after every fourth append
        s = wfile->Flush();
    }

    wfile.reset(nullptr);
  }

  unique_ptr<SequentialFile> rfile;     // Read the file
  s = env->NewSequentialFile(fn, &rfile, opt_e);
  assert(s.ok());

  Slice rslice(sample.rbuf, kFsize);
  s = rfile->Read(kFsize, &rslice, sample.scratch);
  assert(s.ok());

  rfile.reset(nullptr);

  size_t nerr = 0;                      // Compare
  for (size_t i = 0; i < kFsize; ++i) {
    assert(sample.wbuf[i] == rslice[i]);
    if ((sample.wbuf[i] != rslice[i]) && \
      ((i < 50) || (i > kFsize-50) || (rslice[i] > 65 || rslice[i] < 90))) {
      ++nerr;
      std::cout << "buf[" << i << "](" << sample.wbuf[i] << ") != "
                << "rbuf[" << i << "](" << rslice[i] << ")" << std::endl;
    }
  }

  return 0;
}


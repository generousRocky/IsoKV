#include <iostream>
#include <sstream>
#include <cassert>

#include "rocksdb/db.h"
#include "rocksdb/utilities/env_registry.h"
#include "env_nvm.h"
#include "args.h"

using namespace rocksdb;

static size_t kMB = 1 << 20;
static size_t kFsize = 16 * kMB;
static char fn[] = "/opt/rocks/test/testfile.log";

#include "env_nvm_t00.cc"

int main(int argc, char *argv[]) {

  Args args(argc, argv, {"delete_existing"});
  if (argc < 2) {
    args.pr_usage();
    return -1;
  }
  args.pr_args();

  std::unique_ptr<Env> env_guard;
  Env *env = NewEnvFromUri("nvm://nvme0n1/opt/rocks/nvm.meta", &env_guard);
  assert(env);

  EnvOptions opt_e;
  Status s;

  Sample sample(kFsize);                // Create buffers
  //sample.fill(65);
  sample.fill();

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

    size_t nbytes_remaining = kFsize;
    size_t nbytes_written = 0;

    while(nbytes_written < kFsize) {
      size_t nbytes = std::min(nbytes_remaining, align);

      Slice wslice(sample.wbuf + nbytes_written, nbytes);
      s = wfile->Append(wslice);
      assert(s.ok());

      nbytes_written += nbytes;
      nbytes_remaining -= nbytes;
    }

    wfile->Truncate(kFsize);
    wfile->Close();
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
    //assert(sample.wbuf[i] == rslice[i]);

    if (sample.wbuf[i] != rslice[i]) {
      ++nerr;
      std::cout << "buf[" << i << "](" << sample.wbuf[i] << ") != "
                << "rbuf[" << i << "](" << rslice[i] << ")" << std::endl;
    }
  }
  std::cerr << "nerr(" << nerr << ")" << std::endl;

  return 0;
}


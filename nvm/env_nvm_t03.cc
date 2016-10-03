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

class Sample {
public:
  Sample(size_t nbytes) : nbytes(nbytes) {
    scratch = new char[nbytes]();
    wbuf = new char[nbytes]();
    rbuf = new char[nbytes]();
  }

  ~Sample(void) {
    delete [] scratch;
    delete [] wbuf;
    delete [] rbuf;
  }

  void fill(char chr) {
    char prefix[] = "++++START++++";
    char suffix[] = "----END----\n";
    strcpy(wbuf, prefix);
    for (size_t i = strlen(prefix); i < nbytes; ++i) {
      wbuf[i] = chr;
    }
    strcpy(wbuf + (kFsize - strlen(suffix)), suffix);
  }

  void fill(void) {
    char prefix[] = "++++START++++";
    char suffix[] = "----END----\n";
    strcpy(wbuf, prefix);
    for (size_t i = strlen(prefix); i < nbytes; ++i) {
      wbuf[i] = (i % 26) + 65;
    }
    strcpy(wbuf + (kFsize - strlen(suffix)), suffix);
  }

  size_t nbytes;
  char *scratch;
  char *wbuf;
  char *rbuf;
};

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

  Sample sample(kFsize);
  sample.fill(65);

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

  unique_ptr<SequentialFile> rfile;
  s = env->NewSequentialFile(fn, &rfile, opt_e);
  assert(s.ok());

  Slice rslice(sample.rbuf, kFsize);
  s = rfile->Read(kFsize, &rslice, sample.scratch);
  assert(s.ok());

  rfile.reset(nullptr);

  size_t nerr = 0;
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


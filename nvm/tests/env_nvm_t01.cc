#include <iostream>
#include <sstream>
#include <cassert>
#include <sys/time.h>

#include "rocksdb/db.h"
#include "rocksdb/utilities/object_registry.h"
#include "env_nvm.h"
#include "args.h"

static size_t start, stop;

static inline size_t wclock_sample(void)
{
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_usec + tv.tv_sec * 1000000;
}

size_t timer_start(void)
{
    start = wclock_sample();
    return start;
}

size_t timer_stop(void)
{
    stop = wclock_sample();
    return stop;
}

double timer_elapsed(void)
{
    return (stop-start)/(double)1000000.0;
}

void timer_pr(const char* tool)
{
    printf("Ran %s, elapsed wall-clock: %lf\n", tool, timer_elapsed());
}

using namespace rocksdb;

static size_t kMB = 1 << 20;
static size_t kFsize = 4000 * kMB;
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
  Env *env = NewCustomObject<Env>("nvm://punits:0-127@nvme0n1/opt/rocks/nvm.meta", &env_guard);
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

  timer_start();

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

  timer_stop();
  timer_pr("Write");

  timer_start();

  unique_ptr<SequentialFile> rfile;     // Read the file
  s = env->NewSequentialFile(fn, &rfile, opt_e);
  assert(s.ok());

  Slice rslice(sample.rbuf, kFsize);
  s = rfile->Read(kFsize, &rslice, sample.scratch);
  assert(s.ok());

  timer_stop();
  timer_pr("Read");

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


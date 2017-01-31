#include <cstdlib>
#include <cstring>
#include "env_nvm.h"

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

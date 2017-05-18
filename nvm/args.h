#include <iostream>

class Args {
public:
  Args(int argc, char* argv[], std::vector<std::string> ps) : ps_(ps), cmd_(argv[0]) {
    for (size_t i = 0; i < ps_.size(); ++i) {
      if (i < (size_t)(argc-1)) {
        args_[ps_[i]] = atoi(argv[i+1]);
        continue;
      }
      args_[ps_[i]] = 0;
    }
  }

  void pr_usage(void) {
    std::cout << cmd_ << " ";
    for (auto p : ps_) {
      std::cout << p << " ";
    }
    std::cout << std::endl;
  }

  void pr_args(void) {
    for (auto p : ps_) {
      std::cout << p << ": " << args_[p] << std::endl;
    }
  }

  int operator[](const std::string& p) { return args_[p]; }

private:
  std::map<std::string, int> args_;
  std::vector<std::string> ps_;
  std::string cmd_;
};


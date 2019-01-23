#ifndef _FILE_MAP_
#define _FILE_MAP_

#include <map>

enum FileType {
  walFile = 0x1,
  level0SSTFile = 0x2,
  normalSSTFile = 0x4,
};

extern std::map<size_t , FileType> FileMap;

#endif



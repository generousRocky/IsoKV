#ifndef _FILE_MAP_
#define _FILE_MAP_

#include <map>
//#include <slice.h>

enum FileType {
  walFile = 0x1,
  level0SSTFile = 0x2,
  normalSSTFile = 0x4,
	gammaFile = 0x08,
};

extern std::map<size_t , FileType> FileMap;
extern std::map<size_t, size_t> ColdFileMap; // <filenumber, num_cold_key>

extern std::map<uint64_t, size_t> KeyCountMap; // <key, access_count>


#endif


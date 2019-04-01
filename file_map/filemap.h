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
extern std::map<size_t, size_t> AccessCntFileMap; // <filenumber, sum access count>

extern std::vector<uint16_t> KeyAccessCount;// 2MB size

#define get_decode_key(key)({\
	char* pos = const_cast<char*>(key.data());																	\
	size_t pos_multi = 1;																												\
	size_t res = 0;																															\
	for(int i=7; i>=0; i--){																										\
		res = res + ( ((pos[i] << ( (8 - i) >> 3 )) & 0xFF) * pos_multi);			\
		pos_multi = pos_multi * 256;																							\
	}																																						\
	res;																																				\
})

#endif


cmake_minimum_required(VERSION 2.8.3)
project(nvmenv CXX)

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DNVM_DBG_ENABLED")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-builtin-memcmp")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DROCKSDB_FALLOCATE_PRESENT")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DGFLAGS=gflags")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DZLIB")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DROCKSDB_MALLOC_USABLE_SIZE")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DROCKSDB_PTHREAD_ADAPTIVE_MUTEX")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DROCKSDB_BACKTRACE")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DROCKSDB_PLATFORM_POSIX")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DROCKSDB_LIB_IO_POSIX")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DOS_LINUX")

set(HEADER_FILES
  args.h
  env_nvm.h)

set(SOURCE_FILES
  env_nvm.cc
  env_nvm_store.cc
  env_nvm_file.cc
  tests/env_nvm_t01.cc
  tests/env_nvm_t02.cc
  tests/env_nvm_t03.cc)

set(SOURCE_FILES_T01
  env_nvm.cc
  env_nvm_store.cc
  env_nvm_file.cc
  tests/env_nvm_t01.cc)

set(SOURCE_FILES_T02
  env_nvm.cc
  env_nvm_store.cc
  env_nvm_file.cc
  tests/env_nvm_t02.cc)

set(SOURCE_FILES_T03
  env_nvm.cc
  env_nvm_store.cc
  env_nvm_file.cc
  tests/env_nvm_t03.cc)

include_directories(../include)
include_directories(../)
include_directories(${CMAKE_SOURCE_DIR})

get_filename_component(LIB_RDB "../librocksdb_debug.a" ABSOLUTE)
if (NOT EXISTS $LIB_RDB)
  get_filename_component(LIB_RDB "../librocksdb.a" ABSOLUTE)
endif()

find_library(LIBLIGHTNVM NAMES lightnvm)

add_executable(env_nvm_t01 ${HEADER_FILES} ${SOURCE_FILES_T01})
target_link_libraries(env_nvm_t01 pthread
	rt gflags z bz2 zstd snappy ${LIB_RDB} pthread ${LIBLIGHTNVM})

add_executable(env_nvm_t02 ${HEADER_FILES} ${SOURCE_FILES_T02})
target_link_libraries(env_nvm_t02 pthread
	rt gflags z bz2 zstd snappy ${LIB_RDB} pthread ${LIBLIGHTNVM})

add_executable(env_nvm_t03 ${HEADER_FILES} ${SOURCE_FILES_T03})
target_link_libraries(env_nvm_t03 pthread
	rt gflags z bz2 zstd snappy ${LIB_RDB} pthread ${LIBLIGHTNVM})

install(TARGETS env_nvm_t01 DESTINATION bin)
install(TARGETS env_nvm_t02 DESTINATION bin)
install(TARGETS env_nvm_t03 DESTINATION bin)

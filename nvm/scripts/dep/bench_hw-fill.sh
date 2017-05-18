#!/bin/bash

if [ $# -ne 2 ]; then
	echo "Usage: <ROCKS_SRC> <DB_PATH>"
	exit 1
fi

ROCKS_SRC=$1
RBENCH_PATH=$2

pushd $ROCKS_SRC

NUM=800
VALUE_SIZE=1048576
#VALUE_SIZE=4096

taskset -c 0-$(nproc) ./db_bench \
--use_existing_db=0 \
--env_uri="nvm://nvme0n1$RBENCH_PATH/nvm.meta" \
--db="$RBENCH_PATH/bench" \
--benchmarks=fillseq \
--num=$NUM \
--value_size=$VALUE_SIZE \
--verify_checksum=1 \
--sync=0 \
--disable_wal=1 \
--compression_type=none \
--compression_ratio=1 \
--mmap_read=0 \
--stats_interval=100000 \
--stats_per_interval=1 \
--disable_data_sync=0 \
--disable_seek_compaction=1 \
--statistics=1 \
--histogram=1 \
--threads=1 \
--open_files=-1 \
--block_size=262144 \
--cache_size=0 \
--writable_file_max_buffer_size=16777216 \
--random_access_max_buffer_size=16777216 \
--bloom_bits=10 \
--cache_numshardbits=4 \
--write_buffer_size=536870912 \
--max_write_buffer_number=5 \
--min_write_buffer_number_to_merge=2 \
--target_file_size_base=2097152000 \
--max_background_compactions=1 \
--max_grandparent_overlap_factor=10 \
--max_bytes_for_level_base=1073741824 \
--min_level_to_compress=-1 \
--num_levels=7 \
--level0_file_num_compaction_trigger=4 \
--level0_slowdown_writes_trigger=20 \
--level0_stop_writes_trigger=24 \
--delete_obsolete_files_period_micros=0

popd

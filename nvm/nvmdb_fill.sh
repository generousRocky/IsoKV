#!/bin/bash

if [ $# -ne 2 ]; then
	echo "Usage: <ROCKS_SRC> <DB_PATH>"
	exit 1
fi

ROCKS_SRC=$1
DB_PATH=$2

pushd $ROCKS_SRC

NUM=1000
VALUE_SIZE=1048576
KEY_SIZE=16

./db_bench \
--env_uri="nvm://nvme0n1/" \
--use_existing_db=0 \
--db=$DB_PATH \
--benchmarks=fillseq \
--verify_checksum=1 \
--sync=0 \
--disable_wal=1 \
--compression_type=none \
--compression_ratio=1 \
--mmap_read=0 \
--stats_interval=1000 \
--stats_per_interval=1 \
--disable_data_sync=0 \
--disable_seek_compaction=1 \
--statistics=1 \
--histogram=1 \
--threads=1 \
--num=$NUM \
--open_files=1 \
--key_size=$KEY_SIZE \
--value_size=$VALUE_SIZE \
--block_size=65536 \
--cache_size=1048576 \
--bloom_bits=10 \
--cache_numshardbits=4 \
--write_buffer_size=134217728 \
--target_file_size_base=262144 \
--max_write_buffer_number=3 \
--max_background_compactions=10 \
--max_grandparent_overlap_factor=10 \
--max_bytes_for_level_base=10485760 \
--min_level_to_compress=2 \
--num_levels=2 \
--level0_file_num_compaction_trigger=4 \
--level0_slowdown_writes_trigger=8 \
--level0_stop_writes_trigger=12 \
--delete_obsolete_files_period_micros=30000

popd

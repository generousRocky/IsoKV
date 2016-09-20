#!/bin/bash

if [ $# -ne 2 ]; then
	echo "Usage: <ROCKS_SRC> <DB_PATH>"
	exit 1
fi

ROCKS_SRC=$1
DB_PATH=$2

# key_size * value_size * 

pushd $ROCKS_SRC

bpl=10485760;overlap=10;mcz=2;del=30000;levels=2;ctrig=4; delay=8; stop=12;
wbn=3; mbc=10; mb=262144;wbs=134217728; dds=0; sync=0; r=100000; t=1;
vs=1048576; bs=65536; cs=1048576; of=1; si=1000;
./db_bench \
--env_uri="nvm://nvme0n1/" \
--benchmarks=fillseq \
--disable_seek_compaction=1 \
--mmap_read=0 \
--statistics=1 \
--histogram=1 \
--num=$r \
--threads=$t \
--value_size=$vs \
--block_size=$bs \
--cache_size=$cs \
--bloom_bits=10 \
--cache_numshardbits=4 \
--open_files=$of \
--verify_checksum=1 \
--db=$DB_PATH \
--sync=$sync \
--disable_wal=1 \
--compression_type=none \
--compression_ratio=0.5 \
--stats_interval=$si \
--disable_data_sync=$dds \
--write_buffer_size=$wbs \
--target_file_size_base=$mb \
--max_write_buffer_number=$wbn \
--max_background_compactions=$mbc \
--level0_file_num_compaction_trigger=$ctrig \
--level0_slowdown_writes_trigger=$delay \
--level0_stop_writes_trigger=$stop \
--num_levels=$levels \
--delete_obsolete_files_period_micros=$del \
--min_level_to_compress=$mcz \
--max_grandparent_overlap_factor=$overlap \
--stats_per_interval=1 \
--max_bytes_for_level_base=$bpl \
--use_existing_db=0

popd

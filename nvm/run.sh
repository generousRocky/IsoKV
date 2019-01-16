#!/usr/bin/env bash

RBENCH_DEV_NAME="nvme0n1"
RBENCH_BENCHMARKS="fillrandom, stats"
RBENCH_DEV_MODE="nvm"

if [ $UID != 0 ]; then
	echo "You don't have sufficient privileges to run this script."
	exit 1
fi

if [ -z "$RBENCH_DEV_NAME" ]; then
	echo "Please set RBENCH_DEV_NAME e.g. RBENCH_DEV_NAME='nvme0n1'"
	exit 1
fi

if [ -z "$RBENCH_BENCHMARKS" ]; then
	RBENCH_BENCHMARKS="fillseq,readwhilewriting,readseq,readrandom"
fi

if [ -z "$RBENCH_USE_EXISTING_DB" ]; then
	RBENCH_USE_EXISTING_DB="0"
fi

DB_ROOT="/opt/rocks"

RBENCH_DB="$DB_ROOT/${RBENCH_DEV_NAME}_${RBENCH_DEV_MODE}"
mkdir -p $RBENCH_DB

case "$RBENCH_DEV_MODE" in
legacy)
	RBENCH_USE_DIRECT_READS=true
	RBENCH_USE_DIRECT_IO_FOR_FLUSH_AND_COMPACTION=true

	if [ "$RBENCH_USE_EXISTING_DB" -eq "0" ]; then
		umount $RBENCH_DB
		mkfs.ext4 -F "/dev/$RBENCH_DEV_NAME"
		mount "/dev/$RBENCH_DEV_NAME" $RBENCH_DB
	fi
;;
pblk)
	RBENCH_USE_DIRECT_READS=true
	RBENCH_USE_DIRECT_IO_FOR_FLUSH_AND_COMPACTION=true

	if [ "$RBENCH_USE_EXISTING_DB" -eq "0" ]; then
		PBLK_BGN=0
		PBLK_END=127
		PBLK_NAME="pblk_${PBLK_BGN}_${PBLK_END}"
		umount $RBENCH_DB
		nvme lnvm remove $PBLK_NAME
		nvme lnvm create -d $RBENCH_DEV_NAME -n $PBLK_NAME -t pblk -b $PBLK_BGN -e $PBLK_END -f
		mkfs.ext4 -F "/dev/$PBLK_NAME"
		mount "/dev/$PBLK_NAME" $RBENCH_DB
	fi
;;
nvm)
	NVM_BGN=0
	NVM_END=127

	RBENCH_ENV_URI="nvm://punits:${NVM_BGN}-${NVM_END}@${RBENCH_DEV_NAME}${DB_ROOT}/${RBENCH_DEV_NAME}_${NVM_BGN}_${NVM_END}.meta"

	if [ "$RBENCH_USE_EXISTING_DB" == "0" ]; then
		rm $RBENCH_DB/*
	fi
;;
*)
	echo "Please set $RBENCH_DEV_NAME to one of: legacy|pblk|nvm"
esac

KB=1024
MB=$((1024 * KB))
GB=$((1024 * MB))

#RBENCH_CMD_PREFIX="taskset -c 0-$(nproc)"
#RBENCH_CMD_PREFIX="valgrind"
RBENCH_BIN="../db_bench"
RBENCH_NUM=500000
RBENCH_VALUE_SIZE=$((1 * MB))
RBENCH_BLOCK_SIZE=$((64 * KB))
RBENCH_BLOOM_BITS=10
RBENCH_CACHE_SIZE=$((1 * MB))
RBENCH_RANDOM_ACCESS_MAX_BUFFER_SIZE=$((128 * MB))
RBENCH_WRITABLE_FILE_MAX_BUFFER_SIZE=$((128 * MB))
RBENCH_WRITE_BUFFER_SIZE=$((2048 * MB))
RBENCH_MAX_WRITE_BUFFER_NUMBER=1
RBENCH_MIN_WRITE_BUFFER_NUMBER_TO_MERGE=1
RBENCH_TARGET_FILE_SIZE_MULTIPLIER=1
RBENCH_TARGET_FILE_SIZE_BASE=$((2048 * MB))
RBENCH_MAX_BYTES_FOR_LEVEL_BASE=$((4096 * MB))
RBENCH_CACHE_NUMSHARDBITS=4
RBENCH_COMPRESSION_RATIO=1
RBENCH_COMPRESSION_TYPE=none
RBENCH_DELETE_OBSOLETE_FILES_PERIOD_MICROS=300000000
RBENCH_DISABLE_SEEK_COMPACTION=1
RBENCH_DISABLE_WAL=0
RBENCH_LEVEL0_FILE_NUM_COMPACTION_TRIGGER=1
RBENCH_LEVEL0_SLOWDOWN_WRITES_TRIGGER=8
RBENCH_LEVEL0_STOP_WRITES_TRIGGER=12
RBENCH_MAX_BACKGROUND_COMPACTIONS=20
RBENCH_MAX_BACKGROUND_FLUSHES=2
RBENCH_MIN_LEVEL_TO_COMPRESS=2
RBENCH_MMAP_READ=0
RBENCH_NUM_LEVELS=6
RBENCH_OPEN_FILES=500000
RBENCH_STATISTICS=1
RBENCH_STATS_INTERVAL=100000
RBENCH_STATS_PER_INTERVAL=1
RBENCH_SYNC=0
RBENCH_THREADS=1
#RBENCH_THREADS=$(nproc)
RBENCH_VERIFY_CHECKSUM=1

source rbench.sh
rbench

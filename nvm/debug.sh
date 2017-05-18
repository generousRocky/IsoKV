#!/usr/bin/env bash
if [ $UID != 0 ]; then
	pushd ../
	git stash clear
	git stash
	git pull --rebase
	git stash apply
	make clean
	EXTRA_CXXFLAGS="-fpermissive" DEBUG_LEVEL=0 /usr/bin/time make static_lib db_bench -j $(nproc)
	popd

	sudo "$0" "$@"
	exit 0
fi

sudo RBENCH_DEV_NAME="nvme1n1" RBENCH_DEV_MODE="legacy" /usr/bin/time ./run.sh 2>&1 | tee /tmp/rdb_nvme1n1_legacy.log
sudo RBENCH_DEV_NAME="nvme0n1" RBENCH_DEV_MODE="nvm"    /usr/bin/time ./run.sh 2>&1 | tee /tmp/rdb_nvme0n1_nvm.log
sudo RBENCH_DEV_NAME="nvme0n1" RBENCH_DEV_MODE="pblk"    /usr/bin/time ./run.sh 2>&1 | tee /tmp/rdb_nvme0n1_nvm.log

#!/usr/bin/env bash
pushd $HOME/git/rocksdb
git pull --rebase
DEBUG_LEVEL=0 /usr/bin/time make static_lib db_bench -j $(nproc)
popd
rm /tmp/*.sst
rm /tmp/rdb_*.log
rm /opt/rocks/nvme0/*
./scripts/run.sh scripts/env_shared.sh ./scripts/env_nvm_nvme0.sh fillseq,readrandom,readseq,readwhilewriting 0 2>&1 | tee -a /tmp/rdb_all.log
#./scripts/run.sh scripts/env_shared.sh ./scripts/env_nvm_nvme0.sh fillseq 0 2>&1 | tee -a /tmp/rdb_fillseq.log
#./scripts/run.sh scripts/env_shared.sh ./scripts/env_nvm_nvme0.sh readseq 1 2>&1 | tee -a /tmp/rdb_readseq.log

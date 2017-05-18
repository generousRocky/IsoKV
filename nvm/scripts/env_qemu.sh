#!/usr/bin/env bash

RBENCH_ENV_URI="nvm://nvme0n1/opt/rocks/nvm.meta"
RBENCH_DB="/opt/rocks/qe"

# Override of env_shared
RBENCH_NUM=32
RBENCH_BIN="$HOME/host/rocksdb/db_bench"

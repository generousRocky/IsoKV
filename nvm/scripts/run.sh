#!/usr/bin/env bash

if [ $# -ne 4 ]; then
	echo "Usage: <DEFAULT_ENV> <ENV> <BENCHMARK> <USE_EXISTING>"
	exit 1
fi

pushd $CI_ROOT
source scripts/cijoe.sh
popd

source $1			# Source in the default/shared arguments
source $2			# Source in "special" db_bench arguments

RBENCH_BENCHMARKS=$3		# Get the benchmarks to run
RBENCH_USE_EXISTING_DB=$4

rbench				# Run them

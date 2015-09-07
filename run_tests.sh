#!/bin/bash

#run this as ROOT !!

trap "exit 1" TERM
export TOP_PID=$$

function check_result
{
	./$@
	if [ $? -ne 0 ] 
	then
		echo "$@ failed"
		kill -s TERM $TOP_PID
	fi
}

check_result nvm_random_access_file_test

for i in {0..9};
do
	check_result nvm_file_manager_test $i
done

check_result nvm_rw_tests
check_result nvm_ftl_save_test
check_result nvm_sequential_file_test
check_result nvm_gc_tests
check_result nvm_write_test
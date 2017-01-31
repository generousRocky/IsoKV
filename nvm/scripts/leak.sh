#!/usr/bin/env bash
echo "scan" > /sys/kernel/debug/kmemleak
echo "scan" > /sys/kernel/debug/kmemleak
echo "clear" > /sys/kernel/debug/kmemleak
cat /sys/kernel/debug/kmemleak

for i in 1 2 3 4
do
	make t01_1
	echo "scan" > /sys/kernel/debug/kmemleak
	echo "scan" > /sys/kernel/debug/kmemleak
	cat /sys/kernel/debug/kmemleak
	echo "clear" > /sys/kernel/debug/kmemleak
done


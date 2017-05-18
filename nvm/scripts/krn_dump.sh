#!/usr/bin/env bash
echo "scan" > /sys/kernel/debug/kmemleak
echo "scan" > /sys/kernel/debug/kmemleak
cat /sys/kernel/debug/kmemleak

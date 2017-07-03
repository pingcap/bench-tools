#!/bin/bash

set -eo pipefail
trap "Fail unexpectedly on line \$LINENO!" ERR

fatal() { echo $@; exit 1; }
usage() {
	fatal "Usage: $0 <bin-file> <test-plan> [db_path]"
}

bin="$1"
plan="$2"
db_pfx="$3"

if [[ -z $bin || -z $plan ]]; then
	usage
fi
[[ -n $db ]] || db_pfx="rocksdb_test"
log=$db_pfx.log

logt() {
	date=$(date +'%H:%M:%S')
	while read line; do
		echo "[$date] $line"
	done
}
logi() {
	while read line; do
		echo "$1 $line"
	done
}

cat $plan | grep -v '^#' | while read line; do
	echo "################################################################## new task ##################################################################" | tee -a $log
	rand=$(date +'%N')
	ts="#$(date +'%s').$rand"
	db="$db_pfx.$rand"
	rm -rf $db

	config=$(echo "$line" | awk '{print $1}')

	echo "start: $config" | logt | logi $ts | tee -a $log
	cat $config | logi $config | logt | logi $ts >> $log

	$bin -d $db -c $config | logi "result: " | logt | logi $ts | tee -a $log
	[[ $? == 0 ]] || fatal "run failed"
done


fastest=`cat $log | grep "result" | grep 'tps:' | awk '{print $1" "$NF}' | sort -nrk 2 | head -n 1`
ts=`echo "$fastest" | awk '{print $1}'`
speed=`echo "$fastest" | awk '{print $2}'`
config=`cat $log | grep "$ts" | grep "start: " | awk '{print $NF}'`
echo
echo "fastest: $ts, tps: $speed, config as: $config"

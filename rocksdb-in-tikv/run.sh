#!/bin/bash

set -eo pipefail
trap "Fail unexpectedly on line \$LINENO!" ERR

bin="$1"
plan="$2"
log="$3"
db="$4"

trace() { echo "--> $@";}
notice() { echo "==> NOTICE: $@";}
warning() { echo "==> WARNING: $@";}
fatal() { echo "==> FATAL: $@"; exit 1;}
usage() {
	fatal "Usage: <run> \$bin-file \$test-plan [\$log-file] [\$db-path]"
}

if [[ -z $bin || -z $plan ]]; then
	usage
fi
[[ -n $log ]] || log="rocksdb_test.log"
[[ -n $db ]] || db="rocksdb_test"

logt() {
	while read line; do
		echo "[$(date +'%H:%M:%S')] $line"
	done
}
logi() {
	while read line; do
		echo "$1 $line"
	done
}

while read line; do
	echo "---------------------------------------------------------------------------------"
	ts="#$(date +'%s%N')"
	precnt=`echo "$line" | awk '{print $1}'`
	cnt=`echo "$line" | awk '{print $2}'`
	cfg=`echo "$line" | awk '{print $3}'`
	rest=`echo "$line" | awk '{ for(i=4; i<=NF; i++) printf $i" ";}'`

	rm -rf "$db"

	echo "start: $cfg" | logt | logi $ts | tee -a $log
	$bin nosyscheck -d $db -n $precnt -c $cfg $rest | logi "prewrite: " | logt | logi $ts | tee -a $log
	[[ $? == 0 ]] || fatal "run failed"
	
	cat $cfg | logi "$ts ------>" >> $log
	$bin nosyscheck -d $db -n $cnt -c $cfg $rest | logi "result: " | logt | logi $ts | tee -a $log
	[[ $? == 0 ]] || fatal "run failed"
done < $plan


fastest=`cat $log | grep "result" | grep 'tps:' | awk '{print $1" "$NF}' | sort -nrk 2 | head -n 1`
ts=`echo "$fastest" | awk '{print $1}'`
speed=`echo "$fastest" | awk '{print $2}'`
cfg=`cat $log | grep "$ts" | grep "start: " | awk '{print $NF}'`
echo
echo "fastest: $ts, tps: $speed, config as: $cfg"

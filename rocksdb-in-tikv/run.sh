#!/bin/bash

set -eo pipefail
trap "Fail unexpectedly on line \$LINENO!" ERR

HL_RED="\e[31;1m"
HL_GREEN="\e[32;1m"
HL_YELLOW="\e[33;1m"
HL_BLUE="\e[34;1m"
NORMAL="\e[0m"
hl_red()    { echo -e "$HL_RED""$@""$NORMAL" >&2; }
hl_green()  { echo -e "$HL_GREEN""$@""$NORMAL" >&2; }
hl_yellow() { echo -e "$HL_YELLOW""$@""$NORMAL" >&2; }
hl_blue()   { echo -e "$HL_BLUE""$@""$NORMAL" >&2; }
trace() { hl_blue "--> $@"; }
notice() { hl_green "==> NOTICE: $@"; }
warning() { hl_yellow "==> WARNING: $@"; }
fatal() { hl_red "==> ERROR: $@"; exit 1; }

bin="$1"
plan="$2"
log="$3"
db="$4"

usage() {
	hl_red "Usage: $0 <bin-file> <test-plan> [log-file] [db-path]" && exit 1
}

if [[ -z $bin || -z $plan ]]; then
	usage
fi
[[ -n $log ]] || log="rocksdb_test.log"
[[ -n $db ]] || db="rocksdb_test"

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

while read line; do
	trace "---------------------------------------------------------------------------------------------"
	ts="#$(date +'%s%N')"
	precnt=`echo "$line" | awk '{print $1}'`
	cnt=`echo "$line" | awk '{print $2}'`
	cfg=`echo "$line" | awk '{print $3}'`
	rest=`echo "$line" | awk '{ for(i=4; i<=NF; i++) printf $i" ";}'`

	rm -rf "$db"

	echo "start: $cfg" | logt | logi $ts | tee -a $log
	$bin -N -d $db -n $precnt -c $cfg $rest | logi "prewrite: " | logt | logi $ts | tee -a $log
	[[ $? == 0 ]] || fatal "run failed"
	
	cat $cfg | logi "$ts ------>" >> $log
	$bin -N -d $db -n $cnt -c $cfg $rest | logi "result: " | logt | logi $ts | tee -a $log
	[[ $? == 0 ]] || fatal "run failed"
done < $plan


fastest=`cat $log | grep "result" | grep 'tps:' | awk '{print $1" "$NF}' | sort -nrk 2 | head -n 1`
ts=`echo "$fastest" | awk '{print $1}'`
speed=`echo "$fastest" | awk '{print $2}'`
cfg=`cat $log | grep "$ts" | grep "start: " | awk '{print $NF}'`
echo
echo "fastest: $ts, tps: $speed, config as: $cfg"

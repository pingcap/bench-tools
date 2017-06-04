bin="$1"
plan="$2"
log="$3"
db="$4"

help="usage: <run> bin-file test-set-file requets-count report-log-file [db-path]"

if [ -z "$log" ]; then
	echo $help >&2
	exit 1
fi
if [ -z "$db" ]; then
	db="testing_db"
fi
if [ ! -f "$bin" ]; then
	echo "bin-file not found: $plan" >&2
	exit 1
fi
if [ ! -f "$plan" ]; then
	echo "test-set-file not found: $plan" >&2
	exit 1
fi

function logt() {
	while read line; do
		set -o pipefail && echo "["`date "+%H:%M:%S"`"] "$line
		if [ $? != 0 ]; then
			exit 1
		fi
	done
}

function logi() {
	while read line; do
		set -o pipefail && echo "$1" $line
		if [ $? != 0 ]; then
			exit 1
		fi
	done
}

while read line; do
	ts="#`date +%s`""`date +%N`"
	cnt=`echo "$line" | awk '{print $1}'`
	cfg1=`echo "$line" | awk '{print $2}'`
	cfg2=`echo "$line" | awk '{print $3}'`
	rest=`echo "$line" | awk '{for(i=4;i<=NF;i++)printf $i" ";}'`
	echo "start: $cfg1 + $cfg2" | logt | logi $ts | tee -a $log
	cat $cfg1  | logi $ts >> $log
	echo "---" | logi $ts >> $log
	cat $cfg2  | logi $ts >> $log
	echo "---" | logi $ts >> $log
	"$bin" nosyscheck -d $db -n $cnt -c $cfg1 -t $cfg2 $rest | logt | logi $ts | tee -a $log
	if [ $? != 0 ]; then
		exit 1
	fi
done < $plan

fastest=`cat $log | grep 'tps:' | awk '{print $NF" "$1}' | sort -nrk 1 | head -n 1`
speed=`echo "$fastest" | awk '{print $1}'`
ts=`echo "$fastest" | awk '{print $2}'`

cfg=`cat $log | grep "$ts" | grep "start: " | awk '{for(i=NF-2;i<=NF;i++)printf $i" ";}'`
echo
echo "fastest: $ts, tps: $speed, config as: $cfg"

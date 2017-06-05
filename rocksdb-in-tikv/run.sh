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
	precnt=`echo "$line" | awk '{print $1}'`
	cnt=`echo "$line" | awk '{print $2}'`
	cfg=`echo "$line" | awk '{print $3}'`
	rest=`echo "$line" | awk '{for(i=4;i<=NF;i++)printf $i" ";}'`

	rm -rf "$db"

	echo "start: $cfg" | logt | logi $ts | tee -a $log
	"$bin" nosyscheck -d $db -n $precnt -c $cfg $rest | logi "prewrite: " | logt | logi $ts | tee -a $log
	if [ $? != 0 ]; then
		echo "run failed" >&2
		exit 1
	fi

	cat $cfg  | logi $ts >> $log
	"$bin" nosyscheck -d $db -n $cnt -c $cfg $rest | logi "result: " | logt | logi $ts | tee -a $log
	if [ $? != 0 ]; then
		echo "run failed" >&2
		exit 1
	fi
done < $plan

fastest=`cat $log | grep "result" | grep 'tps:' | awk '{print $NF" "$1}' | sort -nrk 1 | head -n 1`
if [ -z "$fastest" ]; then
	exit 1
fi

speed=`echo "$fastest" | awk '{print $1}'`
ts=`echo "$fastest" | awk '{print $2}'`

cfg=`cat $log | grep "$ts" | grep "start: " | awk '{print $NF}'`
echo
echo "fastest: $ts, tps: $speed, config as: $cfg"

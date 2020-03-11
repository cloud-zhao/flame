#!/usr/bin/env bash
base=$(cd $(dirname $0);pwd)

function inc(){
	test -f $1 && . $1
}

inc "/etc/profile"
inc "$HOME/.bash_profile"

function trim(){
	echo "$1" | sed 's/^[[:space:]]*//g' | sed 's/[[:space:]]*$//g'
}

function check_var(){
	test "x$1" == "x" && echo "$2" >&2 && exit
}


function build_jar(){
	local path=$1
	local main_jar=$2
	local pi=""
	local i=""
	test ! -d $path && echo "$path non existent" >&2 && exit
	for i in $(ls $path | grep 'jar$' | grep -v "$main_jar")
	do
		if [ "x$pi" == "x" ];then
			pi="-C file://$path/$i "
		else
			pi="$pi -C file://$path/$i "
		fi
	done
	echo "$pi"
}

function get_abs_path(){
	local f=$(cd $(dirname $1);pwd)
	cd $base
	echo $f
}

function get_config(){
	local conf=$1
	local pr=$2
	local key=$3
	local default=$4

	test ! -f $conf && echo "$conf non existent" >&2 && exit

	local v=$(cat $conf | grep "$pr.$key" | grep -vE "^#")
	if [ "x$v" == "x" ];then
		echo "$default"
	else
		trim $(echo "$v" | awk -F '=' '{print $2}')
	fi

}

function check_job_name_valid() {
  local job_name=$1
  field_num=$(echo ${job_name} | awk -F '@' '{print NF}')
  if [[ field_num -lt 4 ]];then
    echo "job_name:${job_name} is not valid, format \$user@\$desc@\$tag@\$version"
    exit 1
  fi
}

function get_app_id(){
	local job_name=$1
	local s=$(yarn application -list 2>&1 | awk -v jn=$job_name '{if(jn==$2){print $1}}')
	echo $s
}

function stop(){
	local jn=$(get_config $1 "flame" "job.name" "")
	local ids=($(get_app_id $jn))
	local i=""

	test ${#ids[@]} -eq 0 && echo "flink job $jn already kill." && exit
	
	for i in ${ids[@]}
	do
		yarn application -kill $i
	done
}

function start(){
	local conf="$(get_abs_path $1)/$(basename $1)"
	local pr="flame.run"

	check_var "$FLINK_HOME" "FLINK_HOME not set."

	local job_name=$(get_config $conf "flame" "job.name" "")

	local ids=($(get_app_id $job_name))
	test ${#ids[@]} -ne 0 && echo "flink job $job_name already exists." && exit

	local flink_cmd="${FLINK_HOME}/bin/flink run"
	local solt=$(get_config $conf $pr "solt" 4)
	local pm=$(get_config $conf $pr "parallelism" 4)
	local job_mm=$(get_config $conf $pr "job-manager-mem" 1024)
	local task_mm=$(get_config $conf $pr "task-manager-mem" 1024)
	local source_param="-ys $solt -p $pm -yjm $job_mm -ytm $task_mm"
	local queue=$(get_config $conf $pr "queue" "realtime")
	local lib_path="$base/lib"
	check_job_name_valid ${job_name}
	local main_class=$(get_config $conf $pr "main.class" 'com.zyb.bigdata.rtsql.LauncherMain')
	local main_jar=$(get_config $conf $pr "main.jar" "$base/lib/core-1.0.jar")
	local jars=$(build_jar "$lib_path" "$(basename $main_jar)")
	local _args="--properties-file $conf"

	local cmd="$flink_cmd -m yarn-cluster -ynm $job_name -yd $source_param -yqu $queue -c $main_class $jars -yt $lib_path $main_jar"
	echo "$cmd $_args"

	$cmd $_args
}

function main(){
	if test $# -eq 1;then
		start $1
	elif test "$2" == "stop";then
		stop $1
	else
		echo "bash $0 config"
	fi
}

main $@


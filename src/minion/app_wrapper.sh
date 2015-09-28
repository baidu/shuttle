#!/bin/bash
set -x
set -o pipefail
user_cmd=$*

if [ "${mapred_task_is_map}" == "true" ]
then
	./input_tool -file=${map_input_file} -offset=${map_input_start} -len=${map_input_length} | $user_cmd
	exit $?
elif [ "${mapred_task_is_map}" == "false" ]
then
	echo "TODO"	
	exit $?
else
	echo "no in shuttle env"
	exit -1
fi



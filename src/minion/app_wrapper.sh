#!/bin/bash
set -x
set -o pipefail
user_cmd=$*

if [ "${mapred_task_is_map}" == "true" ]
then
	./input_tool -file=${map_input_file} \
	-offset=${map_input_start} \
	-len=${map_input_length} | $user_cmd
	exit $?
elif [ "${mapred_task_is_map}" == "false" ]
then
	./shuffle_tool -total=${mapred_map_tasks} \
	-work_dir=${shuffle_work_dir} \
	-reduce_no=${mapred_task_partition} \
	-attempt_id=${mapred_attempt_id} | $user_cmd
	exit $?
else
	echo "not in shuttle env"
	exit -1
fi

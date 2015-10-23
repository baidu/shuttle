#!/bin/bash
set -x
set -o pipefail
user_cmd=$*

if [ "${mapred_task_is_map}" == "true" ]
then
	dfs_flags=""
	if [ "${minion_input_dfs_host}" != "" ]; then
		dfs_flags="-dfs_host=${minion_input_dfs_host} 
		-dfs_port=${minion_input_dfs_port} 
		-dfs_user=${minion_input_dfs_user} 
		-dfs_password=${minion_input_dfs_password}"
	fi 
	format=""
	if [ "${minion_input_format}" != "" ]; then
		format="-format ${minion_input_format}"
	fi
	pipe_style=""
	if [ "${minion_pipe_style}" != "" ]; then
		pipe_style="-pipe ${minion_pipe_style}"
	fi
	./input_tool -file=${map_input_file} \
	-offset=${map_input_start} \
	-len=${map_input_length} ${dfs_flags} ${format} ${pipe_style} | $user_cmd
	exit $?
elif [ "${mapred_task_is_map}" == "false" ]
then
	dfs_flags=""
	if [ "${minion_output_dfs_host}" != "" ]; then
		dfs_flags="-dfs_host=${minion_output_dfs_host} 
		-dfs_port=${minion_output_dfs_port} 
		-dfs_user=${minion_output_dfs_user} 
		-dfs_password=${minion_output_dfs_password}"
	fi
	pipe_style=""
	if [ "${minion_pipe_style}" != "" ]; then
		pipe_style="-pipe ${minion_pipe_style}"
	fi
	./shuffle_tool -total=${mapred_map_tasks} \
	-work_dir=${minion_shuffle_work_dir} \
	-reduce_no=${mapred_task_partition} \
	-attempt_id=${mapred_attempt_id} $dfs_flags $pipe_style | $user_cmd
	exit $?
else
	echo "not in shuttle env"
	exit -1
fi

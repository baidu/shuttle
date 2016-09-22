#!/bin/bash

set -x
set -o pipefail

CUR_DIR="$(cd $(dirname $0); pwd)"
export WORK_DIR="${CUR_DIR}/phase_${mapred_task_partition}_${mapred_attempt_id}"

function run_input() {
    if [ "${minion_identity}" = "alpha" ]; then
        minion_input_param="--function=input \
            --offset=${map_input_start} \
            --len=${map_input_length} \
            --format=${minion_input_format}"
        if [ "${minion_input_nline}" = "true" ]; then
            minion_input_param="${minion_input_nline} --nline"
        fi
    else
        minion_input_param="--function=shuffle \
            --phase=${minion_phase} \
            --no=${mapred_task_partition} \
            --attempt=${mapred_attempt_id} \
            --total=${mapred_pre_tasks}"
    fi
    ${CUR_DIR}/inlet --pipe="${minion_pipe_style}" \
        --host=${minion_input_dfs_host} \
        --port=${minion_input_dfs_port} \
        --user=${minion_input_dfs_user} \
        --password=${minion_input_dfs_password} \
        --type=hdfs \
        --address=${mapred_task_input} \
        ${minion_input_param}
    return $?
}

function jailrun_usercmd() {
    set -x
    ulimit -m ${mapred_memory_limit}
    ulimit -n 10240
    eval ${minion_user_cmd}
    return $?
}

function run_combiner() {
    ${CUR_DIR}/combiner --pipe="${minion_pipe_style}" \
        --cmd="${minion_combiner_cmd}" \
        --partitioner="${minion_partitioner}" \
        --separator="${minion_key_separator}" \
        --key_fields="${minion_key_fields}"
    return $?
}

function run_output() {
    if [ "${minion_identity}" = "omega" ]; then
        minion_output_param="--function=echo \
            --format=${minion_output_format} \
            --no=${mapred_task_partition}"
    else
        minion_output_param="--function=sort \
            --partitioner=${minion_partitioner} \
            --separator=${minion_key_separator} \
            --key_fields=${minion_key_fields} \
            --partition_fields=${minion_partition_fields} \
            --dest_num=${mapred_next_tasks}"
    fi
    ${CUR_DIR}/outlet --pipe="${minion_pipe_style}" \
        --host=${minion_output_dfs_host} \
        --port=${minion_output_dfs_port} \
        --user=${minion_output_dfs_user} \
        --password=${minion_output_dfs_password} \
        --type=hdfs \
        --address=${mapred_task_output} \
        ${minion_output_param}
    return $?
}

function main_stream() {
    if [ "${minion_identity}" = "" ]; then
        >&2 echo "shuttle identity confusing"
        exit -2
    fi
    mkdir $WORK_DIR && cd $WORK_DIR
    if [ "$?" != "0" ]; then
        exit -3
    fi
    run_input | jailrun_usercmd 2>./stderr | run_combiner | run_output
    exit $?
}

main_stream "$@"


#!/bin/bash

the_dir=`dirname "$0"`
the_dir=`cd "$the_dir"; pwd`

nfs_dir=/disk/shuttle
nfs_path=~/Documents

if [[ $1 == *streaming ]]; then
    files=()
    file_detected=0
    packname=mapred_job_default
    for opt in $@; do
        if [ "$file_detected" = "1" ]; then
            files=(${files[@]} $opt)
            file_detected=0
        fi
        if [ "$opt" = "-file" -o "$opt" = "--file" ]; then
            file_detected=1
        fi
        if [[ $opt = mapred.job.name=* ]]; then
            packname=${opt:16}
        fi
    done
    unset file_detected

    packname=$packname-`date +%s`.tar.gz
    tar -czvf "$packname" ${files[@]} >& /dev/null
    if [ "$?" -ne "0" ]; then
        echo 'file options contains inexist file'
        exit -1
    fi
    $nfs_path/NfsShell put --override $packname $nfs_dir

    params=( "$@" )
    for i in `seq $#`; do
        if [ "${params[i]}" = "-file" -o "${params[i]}" = "--file" ]; then
            unset params[i]
            unset params[$((i+1))]
        fi
    done

    set -- "${params[@]}"
    file_param=-file\ $packname

    rm -rf $packname
fi

source $the_dir/shuttle.conf 2> /dev/null

if [ "$nexus_cluster" ]; then
    nexus_param=-nexus\ $nexus_cluster
else
    if [ `ls -A "$the_dir"/ins.flag 2> /dev/null` ]; then
        nexus_param=-nexus-file\ $the_dir/ins.flag
    fi
fi

if [ -z "$nexus_root" ]; then
    nexus_root=-nexus-root\ $nexus_root
fi

$the_dir/shuttle $nexus_param $nexus_root $file_param \
    -jobconf mapred.job.input.host=$input_host \
    -jobconf mapred.job.input.port=$input_port \
    -jobconf mapred.job.input.user=$input_user \
    -jobconf mapred.job.input.password=$input_password \
    -jobconf mapred.job.output.host=$output_host \
    -jobconf mapred.job.output.port=$output_port \
    -jobconf mapred.job.output.user=$output_user \
    -jobconf mapred.job.output.password=$output_password \
    "$@"


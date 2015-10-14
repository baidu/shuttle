#!/bin/bash

the_dir=`dirname "$0"`
the_dir=`cd "$the_dir"; pwd`

file_detected=0
for opt in $@; do
    if [ "$file_detected" = "1" ]; then
        # TODO Push file to a ftp/nfs position
        file_detected=0
    fi
    if [ "$opt" = "-file" -o "$opt" = "--file" ]; then
        file_detected=1
    fi
done
unset file_detected

source $the_dir/shuttle.conf 2> /dev/null

if [ "$nexus_cluster" ]; then
    nexus_param=-nexus\ $nexus_cluster
else
    if [ `ls -A "$the_dir"/ins.flag 2> /dev/null` ]; then
        nexus_param=-nexus-file\ $the_dir/ins.flag
    fi
fi

exec $the_dir/shuttle $nexus_param \
    -jobconf mapred.job.input.host=$input_host \
    -jobconf mapred.job.input.port=$input_port \
    -jobconf mapred.job.input.user=$input_user \
    -jobconf mapred.job.input.password=$input_password \
    -jobconf mapred.job.output.host=$output_host \
    -jobconf mapred.job.output.port=$output_port \
    -jobconf mapred.job.output.user=$output_user \
    -jobconf mapred.job.output.password=$output_password \
    "$@"


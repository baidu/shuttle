#!/bin/bash
set -x
set -o pipefail
./ionice -c 2 -n 7 -p $$

CmdArgs=$*

script_name=`readlink -f $0`
dir_name=`dirname $script_name`

HADOOP_CLIENT_HOME=/tmp/hadoop-client
CACHE_BASE=$dir_name/mapred
SHARED_CACHE_PREFIX=/home/disk2/

if [ -d $SHARED_CACHE_PREFIX/mapred ] ; then
    CACHE_BASE=$SHARED_CACHE_PREFIX/mapred
else
    mkdir $SHARED_CACHE_PREFIX/mapred
    if [ $? -eq 0 ]; then
        CACHE_BASE=$SHARED_CACHE_PREFIX/mapred
    fi 
fi

IsValidHadoop() {
	if [ ! -f ${HADOOP_CLIENT_HOME}/hadoop/libhdfs/libhdfs.so ]; then
		return 1
	fi
	lib_so_count=`ls ${HADOOP_CLIENT_HOME}/hadoop/lib/native/Linux-amd64-64/ | wc -l`
	if [ $lib_so_count -ne 19 ]; then
		return 2
	fi
	return 0
}

DownloadHadoop() {
	IsValidHadoop
	if [ $? -ne 0 ]; then
		./NfsShell get /disk/shuttle/hadoop-client.tar.gz ./hadoop-client.tar.gz
		if [ $? -ne 0 ]; then
			return 1
		fi
		tar -xzf hadoop-client.tar.gz
		if [ $? -ne 0 ]; then
			return 2
		fi
		IsValidHadoop
		if [ $? -ne 0 ]; then
			rm -rf ${HADOOP_CLIENT_HOME}
			mv ./hadoop-client /tmp/
			return $?
		fi
	fi
	return 0
}

DownloadMinionTar() {
	./NfsShell get /disk/shuttle/minion.tar.gz minion.tar.gz
	return $?
}

ExtractMinionTar() {
	tar -xzf minion.tar.gz
	ret=$?
	mv -f ./hadoop-site.xml $HADOOP_CLIENT_HOME/hadoop/conf/
	return $ret
}

DownloadUserTar() {
	if [ "$app_package" == "" ]; then
		echo "need app_pacakge"
		return 1
	fi
	for ((i=0;i<5;i++))
	do
		cache_archive=$( eval echo \$cache_archive_${i} )
		if [ "$cache_archive" != "" ]; then
			cache_archive_addr=`echo $cache_archive | cut -d"#" -f 1`
			cache_archive_dir=`echo $cache_archive | cut -d"#" -f 2`
			if [ "$cache_archive_dir" == "" ]; then
				return 2
			fi
            cache_key=`${HADOOP_CLIENT_HOME}/hadoop/bin/hadoop fs -ls $cache_archive_addr | tail -1 | md5sum | awk '{print \$1}'`
            if [ ! -d $CACHE_BASE/$cache_key ]; then
                tmp_dump_dir="$CACHE_BASE/${cache_key}_`date +%s`_$$"
                mkdir -p $tmp_dump_dir/$cache_archive_dir
                ${HADOOP_CLIENT_HOME}/hadoop/bin/hadoop fs -get $cache_archive_addr $tmp_dump_dir/$cache_archive_dir
                if [ $? -ne 0 ]; then
                    return 3
                fi
                (cd $tmp_dump_dir/$cache_archive_dir && (tar -xzf *.tar.gz || tar -xf *.tar))
                if [ $? -eq 0 ]; then
                    mv $tmp_dump_dir "$CACHE_BASE/${cache_key}"
                else
                    echo "extract failed"
                    return 4
                fi
            fi
            for sub_dir in $( ls "${CACHE_BASE}/${cache_key}/" )
            do
                ln -s "${CACHE_BASE}/${cache_key}/$sub_dir" .
            done
		else
			break
		fi
	done
	local_package=`echo $app_package | awk -F"/" '{print $NF}'`
	./NfsShell get /disk/shuttle/${app_package} ${local_package}
	return $?	
}

ExtractUserTar() {
	tar -xzf ${local_package}
	return $?
}

StartMinon() {
	source hdfs_env.sh > /dev/null 2>&1
	ls . | grep -v '^log$' > common.list
	if [ $? -ne 0 ]; then
		return 1
	fi
	./minion $CmdArgs
	return $?
}

CheckStatus() {
	ret=$1
	msg=$2
	if [ $ret -ne 0 ]; then
		echo $msg
		sleep 60
		exit $ret
	fi
}

DownloadHadoop
CheckStatus $? "download hadoop fail"

DownloadMinionTar
CheckStatus $? "download minion package from DFS fail"

ExtractMinionTar
CheckStatus $? "extract minion package fail"

DownloadUserTar
CheckStatus $? "download user package fail"

ExtractUserTar
CheckStatus $? "extract user package fail"

StartMinon
CheckStatus $? "minion exit without success"

echo "=== Done ==="

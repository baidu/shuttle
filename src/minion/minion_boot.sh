#!/bin/bash
set -x
set -o pipefail

CmdArgs=$*

DownloadMinionTar() {
	./NfsShell get /disk/shuttle/minion.tar.gz minion.tar.gz
	return $?
}

ExtractMinionTar() {
	tar -xzf minion.tar.gz
	return $?
}


DownloadUserTar() {
	if [ "$app_package" == "" ]; then
		echo "need app_pacakge"
		return -1
	fi
	if [ "$cache_archive" != "" ]; then
		cache_archive_addr=`echo $cache_archive | cut -d"#" -f 1`
		cache_archive_dir=`echo $cache_archive | cut -d"#" -f 2`
		if [ "$cache_archive_dir" == "" ]; then
			return -1
		fi
		mkdir $cache_archive_dir
		./hadoop-client/hadoop/bin/hadoop fs -get $cache_archive_addr $cache_archive_dir
		if [ $? -ne 0 ]; then
			return -1
		fi
		(cd $cache_archive_dir && tar -xzvf *.tar.gz)
	fi
	./NfsShell get /disk/shuttle/${app_package} ${app_package}
	return $?	
}

ExtractUserTar() {
	tar -xzvf ${app_package}
	return $?
}

StartMinon() {
	source hdfs_env.sh > /dev/null 2>&1
	./minion $CmdArgs
	return $?
}

CheckStatus() {
	ret=$1
	msg=$2
	if [ $ret -ne 0 ]; then
		echo $msg
		exit $ret
	fi
}

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

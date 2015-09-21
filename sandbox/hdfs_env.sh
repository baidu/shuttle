#!/bin/sh

if [ "$HADOOP_CLIENT_HOME" == "" ]; then
	echo "set env variable HADOOP_CLIENT_HOME first"
	echo "then, type command: \"source hdfs_env.sh\""
	return
fi

export HADOOP_HOME=${HADOOP_CLIENT_HOME}/hadoop
export JAVA_HOME=${HADOOP_CLIENT_HOME}/java6

CLASSPATH=${HADOOP_HOME}/conf
CLASSPATH=${CLASSPATH}:${HADOOP_HOME}

for path in `ls ${HADOOP_HOME}/hadoop-2-*.jar`
do
   CLASSPATH=${CLASSPATH}:$path
done
for path in `ls ${HADOOP_HOME}/lib/*.jar`
do
   CLASSPATH=${CLASSPATH}:$path
done
CLASSPATH=${CLASSPATH}:${HADOOP_HOME}/lib/jetty-ext/commons-el.jar
CLASSPATH=${CLASSPATH}:${HADOOP_HOME}/lib/jetty-ext/jasper-compiler.jar
CLASSPATH=${CLASSPATH}:${HADOOP_HOME}/lib/jetty-ext/jasper-runtime.jar
export CLASSPATH=${CLASSPATH}:${HADOOP_HOME}/lib/jetty-ext/jsp-api.jar

export HADOOP_LIB_DIR=${HADOOP_HOME}/lib

export LD_LIBRARY_PATH=${HADOOP_HOME}/libhdfs:${JAVA_HOME}/jre/lib/amd64:${JAVA_HOME}/jre/lib/amd64/native_threads:${JAVA_HOME}/jre/lib/amd64/server:$LD_LIBRARY_PATH:${HADOOP_HOME}/lib:${HADOOP_HOME}/lib/native/Linux-amd64-64

export LIBHDFS_OPTS="-Xmx1600m -XX:NewSize=196m"


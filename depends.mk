# This file is used to record path of dependencies
# Modify following variables to proper dependency directory
# shuttle dependencies:
#   galaxy : for job submitting
#   boost : for library functions
#   libhdfs & jvm : for read/write on HDFS
#
# Following dependencies are also needed but will be handled by Galaxy:
#   sofa-pbrpc : for network communication
#   protobuf : for network protocol
#   gflags : for command line parse in master/minion tools
#   snappy : for data compression/uncompression
#   gtest(optional) : for unittest
#

# Galaxy
GALAXY_DIR=./galaxy

# Boost
BOOST_HEADER_DIR=./galaxy/thirdparty/boost_1_57_0

# HDFS library
LIB_HDFS_DIR=../../../inf/computing/libhdfs

# JVM Library
JVM_LIB_DIR=../../../inf/computing/java6/jre/lib/amd64/server


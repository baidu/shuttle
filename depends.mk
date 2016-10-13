# This file is used to record path of dependencies
# Modify following variables to proper dependency directory
# shuttle dependencies:
#   sofa-pbrpc : for network communication
#   protobuf : for network protocol
#   boost : for library functions
#   gflags : for command line parse in master/minion tools
#   snappy : for data compression/uncompression
#   libhdfs : for read/write on HDFS
#   galaxy : for job submitting
#   gtest(optional) : for unittest
#

# sofa-pbrpc
SOFA_PBRPC_DIR=../../opensource/sofa-pbrpc

# Protocol buffer
PROTOBUF_DIR=../../../third-64/protobuf

# Boost
BOOST_HEADER_DIR=../../../third-64/boost/include

# GFlags
GFLAGS_DIR=../../../third-64/gflags

# Snappy
SNAPPY_DIR=../../../third-64/snappy

# HDFS library
LIB_HDFS_DIR=../../inf/computing/libhdfs

# Galaxy
GALAXY_DIR=./galaxy

# GTest
GTEST_DIR=../../../third-64/gtest


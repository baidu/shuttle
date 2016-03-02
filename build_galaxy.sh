#!/bin/bash
git clone https://github.com/baidu/galaxy
if [ $? -ne 0 ]; then
	echo "faild to clone galaxy reposiotry"
	exit -1
fi
( cd galaxy && sh -x ./build.sh )
if [ $? -ne 0 ]; then
	echo "build depends of galaxy failed"
	exit -1
fi
cd galaxy && make -j 8


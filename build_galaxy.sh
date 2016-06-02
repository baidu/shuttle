#!/bin/bash
git clone https://github.com/baidu/galaxy
if [ $? -ne 0 ]; then
	echo "faild to clone galaxy reposiotry"
	exit -1
fi
( cd galaxy && git checkout galaxy3 && sh -x ./build.sh )
if [ $? -ne 0 ]; then
	echo "build galaxy3 failed"
	exit -1
fi


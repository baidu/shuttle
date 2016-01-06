#!/bin/bash
git clone https://github.com/fxsjy/galaxy
if [ $? -ne - ]; then
	echo "faild to clone galaxy reposiotry"
	exit -1
fi
(cd galaxy && git checkout old_sdk_for_shuttle)
cd galaxy && sh -x ./build4internal.sh
if [ $? -ne 0 ]; then
	echo "build depends of galaxy failed"
	exit -1
fi
cd galaxy && make -j 8


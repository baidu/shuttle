#!/bin/bash
if [ ! -e "./galaxy" ]; then
	git clone https://github.com/baidu/galaxy
	if [ $? -ne 0 ]; then
		echo "faild to clone galaxy reposiotry"
		exit -1
	fi
fi
( cd galaxy && git checkout galaxy3 && sh -x ./build.sh )
if [ $? -ne 0 ]; then
	echo "build galaxy3 failed"
	exit -1
fi

# Following lines are used for internal compiling
# Please ignore them
if [ "$(which comake2)" != "" ]; then
	# Internal build
	touch COMAKE
	echo "WORKROOT('../../../')" > COMAKE
	echo "CONFIGS('inf/computing/libhdfs@trunk@COMAKE')" >> COMAKE
	comake2 -UB
	if [ $? -ne 0 ]; then
		echo "comake operation failed"
		exit -1
	fi
fi
echo "dependencies ok"


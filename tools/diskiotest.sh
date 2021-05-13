#!/bin/bash
if [ -d "$1" ]; then
	dd oflag=direct,nonblock if=/dev/zero of=$1/file1.file bs=512M count=1 > diskwrite_result.log  2>&1
	cat diskwrite_result.log 
	echo -e "\n"
	dd iflag=direct,nonblock if=$1/file1.file bs=512M of=/dev/null > diskread_result.log 2>&1
	cat diskread_result.log
	rm -f $1/file1.file
	rm -rf diskwrite_result.log diskread_result.log
	#echo "The write & read results are placed in diskwrite_result & diskread_result in the current path"
else
	echo "The directory '$1' you input does not exist"
fi

#!/bin/bash
mode=$1

function usage() {
        echo "Please input right mode: striped, fsdax or devdax"
        exit 1
}

if [ -z $mode ]; then
	usage
elif [ $mode == "--help" ] || [ $mode == "-h" ]; then
	usage
elif [ $mode != "devdax" ] && [ $mode != "striped" ] && [ $mode != "fsdax" ]; then
	usage	
fi

if [ ! -z "$(df -h | grep /dev/pmem0)" ]; then
	sudo umount /dev/pmem0
fi

if [ ! -z "$(df -h | grep /dev/pmem1)" ]; then
        sudo umount /dev/pmem1
fi

if [ ! -z "$(df -h | grep /dev/mapper/striped-pmem)" ]; then
	sudo umount /dev/mapper/striped-pmem
	sudo dmsetup remove striped-pmem
fi

command -v ndctl >/dev/null 2>&1 || { echo >&2 "I require ndctl but it's not installed.  Aborting."; exit 1; }
ndctl destroy-namespace -f all
command -v ipmctl >/dev/null 2>&1 || { echo >&2 "I require ipmctl but it's not installed.  Aborting."; exit 1; }
ipmctl show -region
if [ "$?" -ne 0 ]; then 
	echo "No PMEM available";
	exit 1	
fi

if [ $mode == "striped" ] || [ $mode == "fsdax" ]; then
	ndctl create-namespace -m fsdax -r region0
	ndctl create-namespace -m fsdax -r region1
	if [ $mode == "fsdax" ]; then
		echo y | sudo mkfs.ext4 /dev/pmem0
		echo y | sudo mkfs.ext4 /dev/pmem1
		sudo mkdir -p /mnt/pmem0
		sudo mkdir -p /mnt/pmem1
		sudo mount -o dax /dev/pmem0 /mnt/pmem0
		sudo mount -o dax /dev/pmem1 /mnt/pmem1
	else 
		sudo echo -e "0 $(( `sudo blockdev --getsz /dev/pmem0` + `sudo blockdev --getsz /dev/pmem0` )) striped 2 4096 /dev/pmem0 0 /dev/pmem1 0" | sudo dmsetup create striped-pmem
		sudo mkfs.ext4 -b 4096 -E stride=512 -F /dev/mapper/striped-pmem
		sudo mkdir -p /mnt/pmem
		sudo mount -o dax /dev/mapper/striped-pmem /mnt/pmem
	fi
elif [ $mode == "devdax" ]; then
	region0_size=$(ndctl list -r region0 | grep "\"size" | awk -F ':'  '{print $2}' | awk '{sub(/.$/,"")}1')
	n0=`expr $region0_size / 1024 / 1024 / 1024 / 120`
	while [ $n0 -ge 1 ]
	do
		ndctl create-namespace -m devdax -r region0 -s 240g
		ndctl create-namespace -m devdax -r region1 -s 240g
		let n0--
	done
	device_list=`echo $(ls /dev/dax*)`
    if [ "$device_list" == "/dev/dax0.0 /dev/dax0.1 /dev/dax1.0 /dev/dax1.1" ];
    then
        exit 0
    else
        exit 1
    fi
fi

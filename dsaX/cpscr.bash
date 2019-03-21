#!/bin/bash
#

# script to copy second-last file from input dir to output dir on dsa-storage

machine=$1

while true; do

    # get dir from UTC_START
    out_dir=`sed -n 1p /mnt/nfs/runtime/UTC_START.txt`
    ssh dsa-storage "source ~/.bashrc; if [ ! -d /mnt/data/${machine}/${out_dir} ]; then mkdir /mnt/data/${machine}/${out_dir}; fi"
    ssh dsa-storage "source ~/.bashrc; rm -rf /mnt/data/${machine}/${out_dir}/tmp"
    ssh dsa-storage "source ~/.bashrc; mkdir /mnt/data/${machine}/${out_dir}/tmp"
    
    # copy correlator data
    if [ ! $machine == "dsa5" ]; then

	n=1
	let n=$(ls -rt /home/user/data/run1/*.fits | wc -l)
	if [ ${n} -gt 1 ] 
	then
	    
	    toCopy=`ls -drt /home/user/data/run1/* | tail -n 2 | head -n 1`
	    echo "Copying $toCopy"
	    scp ${toCopy} user@dsa-storage:/mnt/data/${machine}/${out_dir}/tmp
	    ssh dsa-storage "source ~/.bashrc; mv /mnt/data/${machine}/${out_dir}/tmp/* /mnt/data/${machine}/${out_dir}/"
	    rm -rf ${toCopy}
	   
	fi

    fi

    # copy triggered data

    n=1
    let n=$(ls -rt /home/user/data/raw/fl*.out | wc -l)
    if [ ${n} -gt 1 ] 
    then
	
	toCopy=`ls -drt /home/user/data/raw/fl*.out | tail -n 2 | head -n 1`
	echo "Copying $toCopy"
	scp ${toCopy} user@dsa-storage:/mnt/data/${machine}/${out_dir}/tmp
	ssh dsa-storage "source ~/.bashrc; mv /mnt/data/${machine}/${out_dir}/tmp/* /mnt/data/${machine}/${out_dir}/"
	rm -rf ${toCopy}
	
    fi
    
    sleep 20

done



    

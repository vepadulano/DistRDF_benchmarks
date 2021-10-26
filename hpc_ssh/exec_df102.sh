#!/bin/bash

parsed_hostnames=`scontrol show hostnames $SLURM_JOB_NODELIST | tr '\n' ',' | sed 's/.$//'`
nprocs=$SLURM_CPUS_PER_TASK
npartitions=$(( (SLURM_JOB_NUM_NODES-1) * SLURM_CPUS_PER_TASK * 4 ))
nfiles=10
ntests=3

echo "Running $ntests tests with:"
echo "- Hosts: ${parsed_hostnames}"
echo "- nprocs: $nprocs"
echo "- nfiles: $nfiles"
echo "- npartitions: $npartitions"

killall python3
sleep 2
python3 /hpcscratch/user/etejedor/DistRDF_tests/df102.py "${parsed_hostnames}" $nprocs $nfiles $npartitions $ntests

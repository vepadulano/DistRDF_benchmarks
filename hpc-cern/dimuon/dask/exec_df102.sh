#!/bin/bash

# This suppose that the correct ROOT installation
# is already sourced at login time in the ~/.bashrc file
echo "ROOT Installation: " `which root.exe`
echo "Python executable: " `which python`

parsed_hostnames=`scontrol show hostnames $SLURM_JOB_NODELIST | tr '\n' ',' | sed 's/.$//'`

IFS=',' read -r -a array <<< "$parsed_hostnames"
for node in "${array[@]}"
do
    # /hpcscratch is mounted to a shared filesystem, copy the dimuon dataset file
    # to a /tmp location which each computing node has mounted to the local SSD.
    ssh -o StrictHostKeyChecking=no $node 'if [ ! -e "/tmp/vpadulan/Run2012BC_DoubleMuParked_Muons.root" ]; then cp /hpcscratch/user/vpadulan/data/Run2012BC_DoubleMuParked_Muons.root /tmp/vpadulan; fi'
    # Make 3999 symbolic links to reach 4000 files.
    ssh -o StrictHostKeyChecking=no $node 'for i in `seq 1 3999`; do ln -s /tmp/vpadulan/Run2012BC_DoubleMuParked_Muons.root "/tmp/vpadulan/Run2012BC_DoubleMuParked_Muons_${i}.root"; done'
    echo "COMPLETED COPIES FOR NODE $node"
done

ncores_per_node=$SLURM_CPUS_PER_TASK

ncores_total=$(( (SLURM_JOB_NUM_NODES-1) * ncores_per_node ))

nfiles=4000
ntests=3

for npartitions_per_core in 1 4:
do
	npartitions=$(( ncores_total * npartitions_per_core ))
	echo "Running $ntests tests with:"
	echo "- Hosts: ${parsed_hostnames}"
	echo "- ncores_per_node: $ncores_per_node"
	echo "- nfiles: $nfiles"
	echo "- npartitions (total): $npartitions"
	echo "- ncores (total): $ncores_total"
        echo ${parsed_hostnames}

	python $PWD/df102.py "${parsed_hostnames}" $ncores_per_node $nfiles $npartitions $ntests
done

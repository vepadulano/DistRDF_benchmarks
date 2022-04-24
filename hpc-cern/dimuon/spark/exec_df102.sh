#!/bin/bash

# This supposes that the correct ROOT installation
# is already sourced at login time in the ~/.bashrc file
echo "Python executable: " `which python`
echo "ROOT installation: " `which root.exe`

parsed_hostnames=`scontrol show hostnames $SLURM_JOB_NODELIST | tr '\n' ',' | sed 's/.$//'`
nodes=$((SLURM_JOB_NUM_NODES-1))
ncores_per_node=$SLURM_CPUS_PER_TASK
ncores_total=$(( nodes * ncores_per_node ))
memorypernode=$(($corespernode * 4))

nfiles=4000
ntests=3

IFS=',' read -r -a array <<< "$parsed_hostnames"
master="${array[0]}"
workers=("${array[@]:1}")

# Copy dataset and soft link according to nfiles
for node in "${array[@]}"
do
    # /hpcscratch is mounted to a shared filesystem, copy the dimuon dataset file
    # to a /tmp location which each computing node has mounted to the local SSD.
    ssh -o StrictHostKeyChecking=no $node "if [ ! -e '/tmp/vpadulan/Run2012BC_DoubleMuParked_Muons.root' ]; then cp /hpcscratch/user/vpadulan/data/Run2012BC_DoubleMuParked_Muons.root /tmp/vpadulan; fi"
    ssh -o StrictHostKeyChecking=no $node 'for i in `seq 1 '"$(($nfiles-1))"'`; do ln -s /tmp/vpadulan/Run2012BC_DoubleMuParked_Muons.root "/tmp/vpadulan/Run2012BC_DoubleMuParked_Muons_${i}.root"; done'
    echo "COMPLETED COPIES FOR NODE $node"
done

# Start Spark master
ssh -o StrictHostKeyChecking=no $master "/hpcscratch/user/vpadulan/spark/launch-scripts/start_master.sh"
# Start Spark workers
for node in "${workers[@]}"
do
    ssh -o StrictHostKeyChecking=no $node "/hpcscratch/user/vpadulan/spark/launch-scripts/start_worker.sh $master $corespernode $memorypernode"
done
echo "Created Spark cluster"


for npartitions_per_core in 1 4
do
	npartitions=$(( ncores_total * npartitions_per_core ))

	echo "Running $ntests tests with:"
	echo "- Hosts: ${parsed_hostnames}"
	echo "- ncores_per_node: $ncores_per_node"
	echo "- nfiles: $nfiles"
	echo "- npartitions (total): $npartitions"
	echo "- ncores (total): $ncores_total"

	python $PWD/df102.py $master $nodes $ncores_per_node $nfiles $npartitions $ntests
done

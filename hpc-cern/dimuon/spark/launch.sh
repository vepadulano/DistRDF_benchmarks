#!/bin/bash

ncores_per_node=32

LOGS_DIR=logs
mkdir -p $LOGS_DIR

for ncores in 1 #16 32 64 128 256 512 1024 2048
do
    # Set parameters
    nnodes=`echo "($ncores + $ncores_per_node - 1) / $ncores_per_node" | bc`
    nnodes=$(( nnodes + 1 )) # add scheduler node

    if [ "$ncores" -ge "$ncores_per_node" ]
    then
        ncores_per_task=$ncores_per_node
    else
        ncores_per_task=$ncores
    fi

    mem=$(( 4 * ncores_per_task ))

    job_name="distrdf_dimuon_spark_${ncores}"

    time="24:00:00"

    output="$LOGS_DIR/${job_name}_%j.out"
    error="$LOGS_DIR/${job_name}_%j.err"

    queue=photon

    exec_script=exec_df102.sh

    # Add to queue 
    sbatch -p $queue --exclusive -N $nnodes -c $ncores_per_task --ntasks-per-node 1 --mem="${mem}G" --job-name $job_name --time $time --output $output --error $error $exec_script
done

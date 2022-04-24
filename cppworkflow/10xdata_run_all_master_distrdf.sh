#!/bin/bash

PYTHON=`which python3`
export PYTHONPATH=benchmarks:$PYTHONPATH

TIMES_DIR=time_results/10xdata/master_distrdf/
NTESTS=5

mkdir -p $TIMES_DIR

for benchmark_name in df102_NanoAODDimuonAnalysis_10xdata df103_NanoAODHiggsAnalysis_10xdata df104_HiggsToTwoPhotons_10xdata
do
  for npartitions in 1 4 8 16
  do
    echo "Running benchmark $benchmark_name with $npartitions partition(s)"
    TIME_FILENAME=$TIMES_DIR/${benchmark_name}__${npartitions}
    for i in `seq $NTESTS`
    do
      $PYTHON launch.py --benchmark $benchmark_name --npartitions $npartitions 1>> ${TIME_FILENAME}.out 2>> ${TIME_FILENAME}.err
    done
  done
done

# Cleanup
rm -f rdfworkflow_*
rm *.pdf 

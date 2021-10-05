#!/bin/bash

source $HOME/.bashrc
conda activate rootdev-py38
source ../etejedor-root/install_cppworkflowtest/bin/thisroot.sh

TIMES_DIR=time_results/1xdata/patch_lambdas
NTESTS=5
BENCHMARK_DIR=benchmarks

mkdir -p $TIMES_DIR

for benchmark_name in df102_NanoAODDimuonAnalysis_lambdas df103_NanoAODHiggsAnalysis_lambdas df104_HiggsToTwoPhotons_lambdas
do
  TIME_FILENAME=$TIMES_DIR/${benchmark_name}
  for j in `seq $NTESTS`
  do
    echo "Compiling benchmark $benchmark_name" >> ${TIME_FILENAME}.out
    start_time="$(date -u +%s.%N)"
    g++ -O3 -o $BENCHMARK_DIR/$benchmark_name.o $BENCHMARK_DIR/$benchmark_name.cxx `root-config --cflags --glibs`
    end_time="$(date -u +%s.%N)"
    elapsed="$(bc <<<"$end_time-$start_time")"
    precision=5
    formatted="$(LC_ALL=C /usr/bin/printf "%.*f\n" "$precision" "$elapsed")"
    printf "Time to compile benchmark: $formatted\n" >> ${TIME_FILENAME}.out
  done
  for i in `seq $NTESTS`
  do
    $BENCHMARK_DIR/$benchmark_name.o 1>> ${TIME_FILENAME}.out 2>> ${TIME_FILENAME}.err
  done
done

# Cleanup
rm *.pdf
rm $BENCHMARK_DIR/*.o

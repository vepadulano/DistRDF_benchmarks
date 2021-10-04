# Launcher module for tests

import argparse
import importlib
import ROOT

def create_spark_context():
    import pyspark
    from pyspark import SparkConf, SparkContext
    conf = SparkConf()
    conf.set('spark.ui.showConsoleProgress', 'false')
    sc = SparkContext('local', conf=conf)

if __name__ == '__main__':
    # Parse args
    parser = argparse.ArgumentParser()
    parser.add_argument('--benchmark', type=str, help='Name of benchmark to run')
    parser.add_argument('--data-dir', dest='data_dir', type=str, default='data', help='Directory with the input data')
    parser.add_argument('--npartitions', type=int, default=1, help='Number of dataset partitions')
    parser.add_argument('--optimized', dest='optimized', action='store_true', help='Run in optimized mode')
    parser.set_defaults(optimized=False)
    args = parser.parse_args()

    # Set batch mode
    ROOT.gROOT.SetBatch(True)

    # Initialize backend
    create_spark_context()

    # Run workflow
    ROOT.RDF.Experimental.Distributed.optimized = args.optimized
    bm = importlib.import_module(args.benchmark)
    for _ in range(5):
        bm.run(args.data_dir, args.npartitions)

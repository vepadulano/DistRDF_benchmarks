# DistRDF version of the RDataFrame benchmark rdataframe_asnumpy 
# - Used DistRDF RDataFrame constructor for Spark
# - Added npartitions parameter for Spark RDataFrame constructor

import os
import numpy as np
import ROOT

RDataFrame = ROOT.RDF.Experimental.Distributed.Spark.RDataFrame

# Simple AsNumpy benchmark processing a minimal amount of data from memory

def asnumpy_simple():
    df = RDataFrame(10)
    x = df.Define('x', 'rdfentry_').AsNumpy(['x'])
    return np.sum(x['x'])  # 45

# Read from a NanoAOD file a scalar type branch with millions of events

def asnumpy_nanoaod_scalar(nanoaod_file):
    df = RDataFrame('Events', nanoaod_file)
    df.AsNumpy(['nMuon'])

# Read from a NanoAOD file a vector type branch with millions of events

def asnumpy_nanoaod_vector(nanoaod_file):
    df = RDataFrame('Events', nanoaod_file)
    df.AsNumpy(['Muon_pt'])

# Read from a flat ntuple file of a small size branches in various configurations

def asnumpy_manybranches(ntuple_file, columns):
    df = RDataFrame('mini', ntuple_file)
    df.AsNumpy(columns)

def get_column_names(ntuple_file, vectors=False, booleans=False, scalars=False):
    '''
    Get column names for the "manybranches" benchmarks

    Arguments:
        vectors: boolean, take columns with vectors (containing rvec in the typename)
        booleans: boolean, take columns with booleans
        scalars: boolean, take columns with other scalar types (containing int and float in the typename)

    Returns:
        vector<string> of column names
    '''
    df = ROOT.RDataFrame('mini', ntuple_file)
    columns = ROOT.std.vector('string')()
    for col in df.GetColumnNames():
        typename = str(df.GetColumnType(col)).lower()
        is_boolean = True if 'bool' in typename else False
        is_vector = True if 'rvec' in typename else False
        is_scalar = True if 'int' in typename or 'float' in typename else False
        if (vectors and is_vector) or (booleans and is_boolean) or (scalars and is_scalar):
            columns.push_back(col)
    return columns

def run(path, npartitions):
    nanoaod_file = os.path.join(path, "Run2012BC_DoubleMuParked_Muons.root")
    ntuple_file = os.path.join(path, "data_A.GamGam.root")

    asnumpy_simple()
    asnumpy_nanoaod_scalar(nanoaod_file)
    #asnumpy_nanoaod_vector(nanoaod_file)
    asnumpy_manybranches(ntuple_file, get_column_names(ntuple_file, booleans=True))
    #asnumpy_manybranches(ntuple_file, get_column_names(ntuple_file, scalars=True))
    #asnumpy_manybranches(ntuple_file, get_column_names(ntuple_file, vectors=True))
    #asnumpy_manybranches(ntuple_file, get_column_names(ntuple_file, vectors=True, scalars=True, booleans=True))

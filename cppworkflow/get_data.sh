#!/bin/bash

DATA_LOCATION="https://root.cern.ch/files/rootbench"

DATA_DIR=data
mkdir $DATA_DIR
cd $DATA_DIR

wget $DATA_LOCATION/data_A.GamGam.root
wget $DATA_LOCATION/data_B.GamGam.root
wget $DATA_LOCATION/data_C.GamGam.root
wget $DATA_LOCATION/data_D.GamGam.root
wget $DATA_LOCATION/mc_343981.ggH125_gamgam.GamGam.root
wget $DATA_LOCATION/mc_345041.VBFH125_gamgam.GamGam.root
wget $DATA_LOCATION/Run2012BC_DoubleMuParked_Muons.root
wget $DATA_LOCATION/Run2012B_DoubleElectron.root
wget $DATA_LOCATION/Run2012B_DoubleMuParked.root
wget $DATA_LOCATION/Run2012C_DoubleElectron.root
wget $DATA_LOCATION/Run2012C_DoubleMuParked.root
wget $DATA_LOCATION/SMHiggsToZZTo4L.root
wget $DATA_LOCATION/ZZTo4mu.root
wget $DATA_LOCATION/ZZTo4e.root

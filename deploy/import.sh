#!/bin/bash

#Setup the environment
source ~/.bash_profile
export DATE=`date +%Y%m%d_%H%M`

# attempt before import, clean up the env.
sqlplus -S $2/$3@$1 @sqlscripts/truncatetargettables.sql

#move the files around.
mkdir /tmp/tdm_dir
rm -rf /tmp/tdm_dir/*
cp *.dmp /tmp/tdm_dir/
ls /tmp/tdm_dir/

 
#Import: this should use parameters for the user and db name
impdp $2/$3@$1 parfile=pars/tdmkeys.par TABLE_EXISTS_ACTION=replace
impdp $2/$3@$1 parfile=pars/tdm.par TABLE_EXISTS_ACTION=replace

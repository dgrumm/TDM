#!/bin/bash

#Setup the environment
source ~/.bash_profile
export DATE=`date +%Y%m%d_%H%M`

OUTPUT_DIR=/tmp/tdm_dir

# delete the old output first.
rm -f $OUTPUT_DIR/*

#Export:  this should use parameters for the user and db name
expdp $2/$3@$1 parfile=pars/tdm_exp.par CONTENT=data_only


#create directory tdm_dir as '/tmp/tdm_dir';
#GRANT read, write ON DIRECTORY tdm_dir TO tdm_user;
#grant exp_full_database to tdm_user;
#grant IMP_FULL_DATABASE to tdm_user;
#--SELECT directory_path FROM dba_directories WHERE directory_name = 'tdm_dir';
#SELECT * FROM dba_directories WHERE directory_name = 'TDM_DIR';
#--drop directory tdm_dir;
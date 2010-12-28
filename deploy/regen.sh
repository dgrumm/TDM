#!/bin/bash
################################################################################
#                                                                              #
# Name        : regen.sh                                                       #
# Description : This script extracts TDM data from the source(VDEV) and write  #
#               into TDM Schema                                                #
#                                                                              #
################################################################################
SUCCESS=0
RUN_TIME=`date +%r`
echo "Regen Started - $RUN_TIME"

#Specify the SQL_DIR
SQL_DIR=./Manual_deploy/TDM/sqlscripts
SQLPLUS_CMD=sqlplus

#DB Username/pwd
USER_NAME=TDM_USER
PWD=TDM_USER

#Specify the schema_names
schema_names=( MCMBRWIP MCMBRAPR MCMGAPWIP MCMGAPAPR MCMONWIP MCMONAPR MCMBGWIP MCMBGAPR MCMATWIP MCMATAPR 
EU_MCMBRWIP EU_MCMBRAPR EU_MCMGAPWIP EU_MCMGAPAPR 
CA_MCMBRWIP CA_MCMBRAPR CA_MCMGAPWIP CA_MCMGAPAPR CA_MCMONWIP CA_MCMONAPR)

DB_INSTANCE=GIDTLSD1

#Run all the extracts in parallel
for i in ${schema_names[@]}
do
$SQLPLUS_CMD $USER_NAME/$PWD@$DB_INSTANCE @$SQL_DIR/regen.sql ${i} N &
eval ${i}_pid=$!
done

#Wait for each brand to complete and capture its exit status
for j in ${schema_names[@]}
do
        eval pid=\$${j}_pid
        wait $pid
        if [ $? -ne 0 ]
        then
                echo "Regen failed for ${j}"
                SUCCESS=1
        fi
done


#Run proc for wip-apr dependency
$SQLPLUS_CMD $USER_NAME/$PWD@$DB_INSTANCE @$SQL_DIR/makewipaprdependency.sql
wait $!
if [ $? -ne 0 ]
then
   echo "Regen failed for MAKEWIPAPRDEPENDENCY"
   SUCCESS=1
fi
RUN_TIME=`date +%r`
if [ $SUCCESS -ne 0 ] 
then
        echo "Regen failed for one or more brands"
        echo "Regen Ended - $RUN_TIME"
        exit 1
else
        echo "Regen completed successfully"
        echo "Regen Ended - $RUN_TIME"
        exit 0
fi


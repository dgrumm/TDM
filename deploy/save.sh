#!/bin/bash
################################################################################
#                                                                              #
# Name        : save.sh                                                        #
# Description : This script saves TDM data into the target DB(VDEV or ISO)     #
#                                                                              #
################################################################################
SUCCESS=0
RUN_TIME=`date +%r`
echo "Save Started - $RUN_TIME"

#Specify the SQL_DIR
SQL_DIR=./Manual_deploy/TDM/sqlscripts
USER_NAME=TDM_USER
PWD=TDM_USER
SQLPLUS_CMD=sqlplus

#Specify Schema Names for ISO
schema_names_GIDSOLD1=( MCMBRAPR MCMGAPAPR MCMONAPR MCMBGAPR MCMATAPR EU_MCMBRAPR EU_MCMGAPAPR CA_MCMBRAPR CA_MCMGAPAPR CA_MCMONAPR )

#Specify Schema Names for VDEV
schema_names_GIDTLSD1=( MCMBRWIP MCMGAPWIP MCMONWIP MCMBGWIP MCMATWIP EU_MCMBRWIP EU_MCMGAPWIP CA_MCMBRWIP CA_MCMGAPWIP CA_MCMONWIP )

#Specify Schema Names for E2E2
schema_names_TDM=( MCMBRWIP MCMGAPWIP MCMONWIP MCMBGWIP MCMATWIP EU_MCMBRWIP EU_MCMGAPWIP CA_MCMBRWIP CA_MCMGAPWIP CA_MCMONWIP )

if [ $# -eq 0 ]
then
   echo "Usage: $0 <DB-INSTANCE> <SCHEMA-NAME>(Optional)>\n"
   exit 1
else
   DB_INSTANCE=$1
   if [ $# -gt 1 ]
   then
       #Read all the Schema Names passed as the command line arguments
       CTR=1
       for ARG in $*
       do
               if [ $CTR -gt 1 ]
               then
                        CTR=`expr $CTR - 1`
                        schema_names[$CTR]=$ARG
               fi
               CTR=`expr $CTR + 2`
       done
    else
        eval schema_names=( "\${schema_names_$DB_INSTANCE[@]}" )
    fi
fi


if [ $DB_INSTANCE == "GIDSOLD1" ]
then
        SQL_FILE=$SQL_DIR/iso_save_brand.sql
        SQL_FILE_PPC=$SQL_DIR/iso_save_ppc.sql
elif [ $DB_INSTANCE == "GIDTLSD1" ]
then
        SQL_FILE=$SQL_DIR/vdev_save_brand.sql
        SQL_FILE_PPC=$SQL_DIR/vdev_save_ppc.sql
	SQL_FILE_TOOLSVC=$SQL_DIR/vdev_save_toolsvc.sql
        #Run Save for TOOLSVC
        echo $SQLPLUS_CMD $USER_NAME/$PWD@$DB_INSTANCE @$SQL_FILE_TOOLSVC &
        $SQLPLUS_CMD $USER_NAME/$PWD@$DB_INSTANCE @$SQL_FILE_TOOLSVC &
        wait $!
        if [ $? -ne 0 ]
        then
           echo "Save failed for Toolsvc"
           SUCCESS=1
        fi
elif [ $DB_INSTANCE == "TDM" ]
then
        SQL_FILE=$SQL_DIR/vdev_save_brand.sql
        SQL_FILE_PPC=$SQL_DIR/vdev_save_ppc.sql        
	SQL_FILE_TOOLSVC=$SQL_DIR/vdev_save_toolsvc.sql
        #Run Save for TOOLSVC
        echo $SQLPLUS_CMD $USER_NAME/$PWD@$DB_INSTANCE @$SQL_FILE_TOOLSVC &
        $SQLPLUS_CMD $USER_NAME/$PWD@$DB_INSTANCE @$SQL_FILE_TOOLSVC &
        wait $!
        if [ $? -ne 0 ]
        then
           echo "Save failed for Toolsvc"
           SUCCESS=1
        fi
fi

#Run all the saves in parallel
for i in ${schema_names[@]}
do
echo $SQLPLUS_CMD $USER_NAME/$PWD@$DB_INSTANCE @$SQL_FILE ${i} &
$SQLPLUS_CMD $USER_NAME/$PWD@$DB_INSTANCE @$SQL_FILE ${i} &
eval ${i}_pid=$!
done

#Wait for each brand to complete and capture its exit status
for j in ${schema_names[@]}
do
        eval pid=\$${j}_pid
        wait $pid
        if [ $? -ne 0 ]
        then
                #echo "Save failed for ${j}"
                SUCCESS=1
        fi
done

#Run Save for PPC
echo $SQLPLUS_CMD $USER_NAME/$PWD@$DB_INSTANCE @$SQL_FILE_PPC &
$SQLPLUS_CMD $USER_NAME/$PWD@$DB_INSTANCE @$SQL_FILE_PPC &
wait $!
if [ $? -ne 0 ]
then
   echo "Save failed for PPC"
   SUCCESS=1
fi

RUN_TIME=`date +%r`
if [ $SUCCESS -ne 0 ]
then
        echo "Save failed for one or more brands"
        echo "Save Ended - $RUN_TIME"
        exit 1
else
        echo "Save completed successfully"
        echo "Save Ended - $RUN_TIME"
        exit 0
fi


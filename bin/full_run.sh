#!/bin/bash
######################################################################################################################################################
# Application Name     : CHURN
# Description          : Run Churn Python Spark Tasks
# Server               : Edge Nodes
# Original Script Name : exportData4C.sh
# Calling Parameters   : NONE
# Files IN             : NONE
# Files OUT            : NONE
# History              : 2018.09.05 - Edited by Laurent Berder
######################################################################################################################################################

source /appl/4C/churn_dev/bin/var_churn4C.sh

# Update source table in Oracle
sqoop eval -Dhadoop.security.credential.provider.path=${db_dest_pass_path} --connect ${db_dest_uri} --username=${db_dest_user} --password-alias=${db_dest_pass_alias} --query "DBMS_MVIEW.REFRESH('SMDT.DD_CUSTATRISK_BK', 'C', atomic_refresh => FALSE)"

# Import data from Oracle to Hive
source ${fs_fourc_path}/${fs_foldername}/bin/importData4C.sh

# Run spark program
source ${fs_fourc_path}/${fs_foldername}/bin/runChurn.sh

# Save data to Oracle
if [ ${ret_code} == 0 ]
then source ${current_dir}/exportData4C.sh
fi

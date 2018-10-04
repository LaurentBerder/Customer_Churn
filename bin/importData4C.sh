#!/bin/bash
######################################################################################################################################################
# Application Name     : CHURN
# Description          : Run Python Sqoop Import Tasks
# Server               : Edge Nodes
# Original Script Name : importData4C.sh
# Calling Parameters   : NONE
# Files IN             : NONE
# Files OUT            : NONE
# History              : 2018.03.14 - Original Version   
######################################################################################################################################################

source ${project_path}/bin/var_churn4C.sh
sqoop_log_file=${fs_logs_path}/${app_str}_sqoop_import_`date '+%Y.%m.%d'`.log
sqoop_log_csv=${fs_logs_path}/${app_str}_sqoop_import_`date '+%Y.%m.%d'`.csv
echo "Date;TableName;ErrorCode;Duration(s);RowCount" >> ${sqoop_log_csv} 

echo "`date '+%Y.%m.%d %H:%M:%S'` - Started ${app_name} Sqoop export tasks on ${current_date} | tee -a ${sqoop_log_file}"

# Import table
drop_hive_table ${hive_dbname} ${hive_table_name}
import_table ${oracle_table_name} ${hive_table_name} 1
ret_code=$?
echo "`date '+%Y.%m.%d %H:%M:%S'` - Finished (with exit code ${ret_code}) Sqoop imports to ${hive_dbname} database" | tee -a ${sqoop_log_file}

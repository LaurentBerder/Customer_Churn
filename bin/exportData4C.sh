#!/bin/bash
######################################################################################################################################################
# Application Name     : CHURN
# Description          : Run Python Sqoop Export Tasks
# Server               : Edge Nodes
# Original Script Name : exportData4C.sh
# Calling Parameters   : NONE
# Files IN             : NONE
# Files OUT            : NONE
# History              : 2018.08.21 - Original Version
######################################################################################################################################################
source ${project_path}/bin/var_churn4C.sh
sqoop_log_file=${fs_logs_path}/${app_str}_sqoop_export_`date '+%Y.%m.%d'`.log
sqoop_log_csv=${fs_logs_path}/${app_str}_sqoop_export_`date '+%Y.%m.%d'`.csv
echo "Saving data to Oracle"
temp_date=`date '+%Y%b%d'`
run_date=`date '+%Y-%m-%d'`
condition="run_date=\"${run_date}\""
hive_table="churn_risk_classification"
oracle_table="CHURN_RISK_CLASSIFICATION"
intermediate_table="${hive_table}_COPY_${temp_date}"
drop_hive_table ${hive_dbname} ${intermediate_table}
create_table_as ${hive_table} ${intermediate_table} "${condition}"
#truncate_table ${oracle_table}
delete_from_table ${oracle_table} "${condition}"
export_table ${intermediate_table} ${oracle_table} 1
ret_code=$?
drop_hive_table ${hive_dbname} ${intermediate_table}
echo "`date '+%Y.%m.%d %H:%M:%S'` - Finished (with exit code ${ret_code}) Sqoop export to table ${oracle_table}" | tee -a ${sqoop_log_file}
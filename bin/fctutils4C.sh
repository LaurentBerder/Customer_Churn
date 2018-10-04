#!/bin/bash
######################################################################################################################################################
# Application           : Common
# Description           : Initialize Application variables before starting any process
# Notes					: 01. Check and keep the "dev" variable empty for non "dev" applications
#						: Beware: function return values using the echo command
# Server                : Edge Nodes
# Original Script Name  : var_${app_name}.sh
# Calling Parameters    : NONE
# Files IN              : NONE
# Files OUT             : NONE
# History               : 2018.01.05 - Original Version   
######################################################################################################################################################

# Common Variables

# Parameters
# Return the run_date from table TEC_PARAMETERS
get_run_date() {
  lines_str=$(beeline -u $hive_url -e "use ${hive_dbname};select VALUE_DATE from tec_parameters WHERE PROGRAMME='GLOBAL' and NAME='Run Date';")
  run_date=$(echo ${lines_str} | grep -o -E '([0-9]+-[0-9]+-[0-9]+)')  
  if [ -z ${run_date} ]
        then			
			run_date_FMTYYYYMMDD=`date '+%Y%m%d'`
			echo `date '+%Y.%m.%d %H:%M:%S'`" - Warning retrieving run_date from TEC_PARAMETERS table failed, using today date instead (${run_date_FMTYYYYMMDD})" >> ${spark_log_file}			
		else
			run_date_FMTYYYYMMDD=$(sed "s/-//g"  <<< $run_date)
			echo `date '+%Y.%m.%d %H:%M:%S'`" - Raw run_date is '${run_date}' reformated to" ${run_date_FMTYYYYMMDD} >> ${spark_log_file}
  fi
  echo ${run_date_FMTYYYYMMDD}
}
# get the run date in "-" format
get_run_dashed_date() {
  lines_str=$(beeline -u $hive_url -e "use ${hive_dbname};select VALUE_DATE from tec_parameters WHERE PROGRAMME='GLOBAL' and NAME='Run Date';")
  run_date=$(echo ${lines_str} | grep -o -E '([0-9]+-[0-9]+-[0-9]+)')  
  if [ -z ${run_date} ]
        then			
			run_date_FMTYYYY_MM_DD=`date '+%Y-%m-%d'`
			echo `date '+%Y.%m.%d %H:%M:%S'`" - Warning retrieving run_date from TEC_PARAMETERS table failed, using today date instead (${run_date_FMTYYYY_MM_DD})" >> ${spark_log_file}			
		else
			run_date_FMTYYYY_MM_DD=${run_date}
			echo `date '+%Y.%m.%d %H:%M:%S'`" - Raw run_date is '${run_date}' reformated to" ${run_date_FMTYYYY_MM_DD} >> ${spark_log_file}
  fi
  echo ${run_date_FMTYYYY_MM_DD}
}
 
drop_hive_table() {
   db_name=$1
   table_name=$2
  echo `date '+%Y.%m.%d %H:%M:%S'`" - Dropping hive table" ${db_name}.${table_name} | tee -a ${spark_log_file} 
  beeline -u $hive_url -e "drop table IF EXISTS ${db_name}.${table_name};"
 }
 # Drop the tables before creating it !!!


renew_ticket(){
  renew_keytab_name=$1
  renew_user_name=$2
  echo  `date '+%Y.%m.%d %H:%M:%S'`" - Destroying/Renewing KRB ticket for '"${renew_user_name}"' with keytab'" ${renew_keytab_name}"'"... | tee -a ${spark_log_file} 
  if [ ! -f ${renew_keytab_name} ]; then
    echo "Keytab file not found"
  else
    kdestroy
	kinit -kt ${renew_keytab_name} ${renew_user_name}
  fi

}


import_table() {
  renew_ticket ${keytab_path} ${user_name}
  table=$1
  hive_table=${2}
  mappers=$3
  other_args=''
  if [ "$4" != '' ]; then
    other_args="--split-by=$4 $5"
  fi

SECONDS=0
echo  `date '+%Y.%m.%d %H:%M:%S'`" - Dropping existing Hive table" ${hive_dbname}.${hive_table} | tee -a ${sqoop_log_file} 
echo  `date '+%Y.%m.%d %H:%M:%S'`" - Options: *** DROP table IF EXISTS ${hive_dbname}.${hive_table} ***" | tee -a ${sqoop_log_file} 
beeline -u $hive_url -e "DROP table IF EXISTS ${hive_dbname}.${hive_table}"
echo `date '+%Y.%m.%d %H:%M:%S'`" - Sqoop import from Oracle table '${table}' to hive" ${hive_dbname}.${hive_table} | tee -a ${sqoop_log_file} 
echo `date '+%Y-%m-%d %H:%M:%S'`" - Options: *** sqoop import -Dmapred.job.queue.name=${queue_main_name} -Dhadoop.security.credential.provider.path=${db_source_pass_path} -Dorg.apache.sqoop.splitter.allow_text_splitter=true --connect ${db_source_uri} --username ${db_source_user} --password-alias ${db_source_pass_alias} --num-mappers $mappers --null-string '\\N' --null-non-string '\\N' --hive-delims-replacement '\0D' --table $table --hcatalog-database ${hive_dbname} --hcatalog-table $hive_table --hcatalog-storage-stanza 'stored as orcfile' --create-hcatalog-table $other_args ***" | tee -a ${log_txt}
sqoop import -Dhadoop.security.credential.provider.path=${db_source_pass_path} -Dorg.apache.sqoop.splitter.allow_text_splitter=true --connect ${db_source_uri} --username ${db_source_user} --password-alias ${db_source_pass_alias} --num-mappers $mappers --null-string '\\N' --null-non-string '\\N' --hive-delims-replacement '\0D' --table $table --hcatalog-database ${hive_dbname} --hcatalog-table $hive_table --hcatalog-storage-stanza 'stored as orcfile' --create-hcatalog-table $other_args
ret_code=$?
  
task_duration=$SECONDS
 lines_str=$(beeline -u $hive_url -e "use ${hive_dbname};select count(*) FROM ${hive_table};")
 row_count=$(echo ${lines_str} | grep -o -E '\s([0-9]+)\s')
 echo `date '+%Y-%m-%d %H:%M:%S'`" - Table ${hive_dbname}.${hive_table} contains ${row_count} rows imported in ${task_duration} seconds" | tee -a ${sqoop_log_file} 
 echo `date '+%Y.%m.%d %H:%M:%S'`";SQOOP IMPORT;"${hive_table}";"${ret_code}";"${task_duration}";"${row_count} >> ${sqoop_log_csv} 
 return ${ret_code}
}


create_table_as() {
  renew_ticket ${keytab_path} ${user_name}
  table_source=$1
  table_dest=$2
  condition=$3
	
SECONDS=0
echo  `date '+%Y.%m.%d %H:%M:%S'`" - Create a table ${table_dest} as select for table ${table_source}" | tee -a ${sqoop_log_file} 
echo  `date '+%Y.%m.%d %H:%M:%S'`" - Options: *** create table ${hive_dbname}.${table_dest} AS select * from ${hive_dbname}.${table_source} where ${condition} ***" | tee -a ${sqoop_log_file} 
beeline -u $hive_url -e "create table ${hive_dbname}.${table_dest} AS select * from ${hive_dbname}.${table_source} where ${condition};"
ret_code=$?
  
task_duration=$SECONDS
 echo `date '+%Y-%m-%d %H:%M:%S'`" - Table ${hive_dbname}.${table_dest} created in ${task_duration} seconds" | tee -a ${sqoop_log_file} 
 echo `date '+%Y.%m.%d %H:%M:%S'`";CREATE TABLE AS;"${table_dest}";"${ret_code}";"${task_duration} >> ${sqoop_log_csv} 
 return ${ret_code}
}


create_view() {
  renew_ticket ${keytab_path} ${user_name}
  table_origin=$1
  view_dest=${2}
  # RUN_DATE=TO_DATE('2018-04-09 00:00:00')
  condition=$3

SECONDS=0
echo  `date '+%Y.%m.%d %H:%M:%S'`" - Dropping view ${view_dest} if exists" | tee -a ${spark_log_file} 
echo  `date '+%Y.%m.%d %H:%M:%S'`" - Options: *** drop view ${hive_dbname}.${view_dest} ***" | tee -a ${spark_log_file} 
beeline -u $hive_url -e "drop view ${hive_dbname}.${view_dest};"

echo  `date '+%Y.%m.%d %H:%M:%S'`" - Creating new view ${view_dest}" | tee -a ${spark_log_file} 
echo  `date '+%Y.%m.%d %H:%M:%S'`" - Options: *** create view ${hive_dbname}.${view_dest} as SELECT * FROM ${hive_dbname}.${table_origin} WHERE ${condition} ***" | tee -a ${spark_log_file} 
beeline -u $hive_url -e "create view ${hive_dbname}.${view_dest} as SELECT * FROM ${hive_dbname}.${table_origin} WHERE ${condition};"
ret_code=$?
  
task_duration=$SECONDS
 echo `date '+%Y-%m-%d %H:%M:%S'`" - View ${hive_dbname}.${view_dest} created in ${task_duration} seconds" | tee -a ${spark_log_file} 
 echo `date '+%Y.%m.%d %H:%M:%S'`";"${view_dest}";"${ret_code}";"${task_duration} >> ${spark_log_csv} 
 return ${ret_code}
}

export_table() {
 renew_ticket ${keytab_path} ${user_name}
 hive_table=$1
 table=$2
 mappers=$3
  
 SECONDS=0
 hive_char=$( printf "\x01" )

 echo `date '+%Y-%m-%d %H:%M:%S'`" - Options: *** sqoop export -Dhadoop.security.credential.provider.path=${db_dest_pass_path} --connect ${db_dest_uri} --username ${db_dest_user} --password-alias ${db_dest_pass_alias} --num-mappers $mappers  --table $table --hcatalog-database ${hive_dbname} --where $condition --hcatalog-table $hive_table --input-fields-terminated-by $hive_char --lines-terminated-by '\n' ***" | tee -a ${spark_log_file} 
 sqoop export -Dhadoop.security.credential.provider.path=${db_dest_pass_path} --connect ${db_dest_uri} --username ${db_dest_user} --password-alias ${db_dest_pass_alias} --num-mappers $mappers  --table $table --hcatalog-database ${hive_dbname} --hcatalog-table $hive_table --input-fields-terminated-by $hive_char --lines-terminated-by '\n'
 ret_code=$?
 echo `date '+%Y-%m-%d %H:%M:%S'`" - Exit code is ${ret_code} while exporting ${hive_table} - Took $SECONDS seconds" | tee -a ${sqoop_log_file}
 echo `date '+%Y.%m.%d %H:%M:%S'`";"${hive_table}";"${ret_code}";"${SECONDS} >> ${sqoop_log_csv} 
 return ${ret_code}
}

truncate_table() {
 renew_ticket ${keytab_path} ${user_name}
 table=$1
  
 SECONDS=0

 echo `date '+%Y-%m-%d %H:%M:%S'`" - Options: *** sqoop eval -Dhadoop.security.credential.provider.path=${db_dest_pass_path} --connect ${db_dest_uri} --username ${db_dest_user} --password-alias ${db_dest_pass_alias} --num-mappers $mappers  --table $table --hcatalog-database ${hive_dbname} --where $condition --hcatalog-table $hive_table --input-fields-terminated-by $hive_char --lines-terminated-by '\n' ***" | tee -a ${spark_log_file} 
 sqoop eval -Dhadoop.security.credential.provider.path=${db_dest_pass_path} --connect ${db_dest_uri} --username=${db_dest_user} --password-alias=${db_dest_pass_alias} --query "TRUNCATE TABLE ${table}"
 ret_code=$?
 echo `date '+%Y-%m-%d %H:%M:%S'`" - Exit code is ${ret_code} while truncating oracle table: ${table} - Took $SECONDS seconds" | tee -a ${sqoop_log_file}
 echo `date '+%Y.%m.%d %H:%M:%S'`";"${table}";"${ret_code}";"${SECONDS} >> ${sqoop_log_csv} 
 return ${ret_code}
}

delete_from_table() {
 renew_ticket ${keytab_path} ${user_name}
 table=$1
 condition=$2

 SECONDS=0

 echo `date '+%Y-%m-%d %H:%M:%S'`" - Options: *** sqoop eval -Dhadoop.security.credential.provider.path=${db_dest_pass_path} --connect ${db_dest_uri} --username ${db_dest_user} --password-alias ${db_dest_pass_alias} --num-mappers $mappers  --table $table --hcatalog-database ${hive_dbname} --where $condition --hcatalog-table $hive_table --input-fields-terminated-by $hive_char --lines-terminated-by '\n' ***" | tee -a ${spark_log_file}
 sqoop eval -Dhadoop.security.credential.provider.path=${db_dest_pass_path} --connect ${db_dest_uri} --username=${db_dest_user} --password-alias=${db_dest_pass_alias} --query "DELETE FROM ${table} WHERE ${condition}"
 ret_code=$?
 echo `date '+%Y-%m-%d %H:%M:%S'`" - Exit code is ${ret_code} while truncating oracle table: ${table} - Took $SECONDS seconds" | tee -a ${sqoop_log_file}
 echo `date '+%Y.%m.%d %H:%M:%S'`";"${table}";"${ret_code}";"${SECONDS} >> ${sqoop_log_csv}
 return ${ret_code}
}


submit_spark(){
   renew_ticket ${keytab_path} ${user_name}
  local py_program=$1
  local run_date=$2
  local database=$3
  local table=$4
  other_args=''
  if [ "$5" != '' ]; then
    other_args="$5 $6"
  fi

SECONDS=0
spark_options=" --files /usr/hdp/current/spark2-client/conf/hive-site.xml --num-executors ${spark_num_exec} --executor-cores ${spark_exec_core}  --executor-memory ${spark_exec_mem}  --driver-memory ${spark_drv_mem} --conf spark.driver.maxResultSize=${spark_drv_max_res} --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.kryoserializer.buffer.max=${spark_kryo_buf_max}"
pyt_env_opt=" --master yarn --deploy-mode client --py-files ${fs_py_files}"
echo `date '+%Y.%m.%d %H:%M:%S'`" - Running spark command for program ${py_program}" | tee -a ${spark_log_file}
echo `date '+%Y-%m-%d %H:%M:%S'`" - Options: *** $spark_options  $pyt_env_opt  ${fs_sources_path}/${py_program} --date $run_date --database $hive_dbname --churn_table $table ${other_args} ***" | tee -a ${spark_log_file}

spark-submit $spark_options  $pyt_env_opt  ${fs_sources_path}/${py_program} --date $run_date --database $hive_dbname --churn_table $table ${other_args}
ret_code=$?

echo `date '+%Y-%m-%d %H:%M:%S'`" - Exit code is ${ret_code} while running ${fs_sources_path}/${py_program} - Took $SECONDS seconds" | tee -a ${spark_log_file}
echo `date '+%Y.%m.%d %H:%M:%S'`";"${py_program}";"${ret_code}";"$SECONDS >> ${spark_log_csv}
return ${ret_code}
}

# This will avoid loading functions
main() {
    echo "Running main"
}
  
display_variables(){
echo `date '+%Y.%m.%d %H:%M:%S'`" - `date '+%Y.%m.%d %H:%M:%S'`- Importing Oracle data from '"${db_source_uri}"' using user '"$db_source_user"'" | tee -a ${spark_log_file}
echo `date '+%Y.%m.%d %H:%M:%S'`" - `date '+%Y.%m.%d %H:%M:%S'` - Sqoop password file is '"${db_source_pass_alias}"'" | tee -a ${spark_log_file}
}

if [[ "${#BASH_SOURCE[@]}" -eq 1 ]]; then
    main "$@"
fi

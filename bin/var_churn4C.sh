#!/bin/bash
######################################################################################################################################################
# Application           : CHURN
# Description           : Initialize Application variables before starting any process
# Notes					: 01. Check and keep the "dev" variable empty for non "dev" applications
# Server                : Edge Nodes
# Original Script Name  : var_${app_name}.sh
# Calling Parameters    : NONE
# Files IN              : NONE
# Files OUT             : NONE
# History               : 2018.08.13 - Original Version   
######################################################################################################################################################

# Common Variables
cluster="DEV-4C"
realm=IT.LOCAL
fs_fourc_path=/appl/4C
export SPARK_MAJOR_VERSION=2
hive_url=""
dev=_dev

# Application Variables
app_name="CHURN"
user_name="churn-dev"
app_str="churn"
fs_foldername=${app_str}${dev}
hdfs_foldername=${app_str}${dev}
hive_dbname=${app_str}${dev}

queue_main_name="default"
queue_second_name="default"
#TODO: définir le hdfs_functional_path
hdfs_functional_path=""

#PATHS
fs_logs_path=${fs_fourc_path}/${fs_foldername}/logs/
hdfs_app_path="hdfs://${cluster}${hdfs_functional_path}/apps/${hdfs_foldername}/"
hdfs_app_path_without_cluster="hdfs://${hdfs_functional_path}/apps/${hdfs_foldername}/"
fs_sources_path="${fs_fourc_path}/${fs_foldername}/src/"
#hdfs_models_path="hdfs://${cluster}/${hdfs_functional_path}/internal_data/${hdfs_foldername}/models/"
fs_venv_path="${fs_fourc_path}/${fs_foldername}/tools/venv"

#DATABASE SOURCE/IMPORT
db_source_uri=""
db_source_user=""
db_source_pass_path=""
db_source_pass_alias=""

#DATABASE DESTINATION/EXPORT
db_dest_uri=""
db_dest_user=""
db_dest_pass_path=
db_dest_pass_alias=""

#HIVE DATABASE
hive_db_path=${hdfs_functional_path}/internal_data/${hdfs_foldername}/database


# SPARK parameters (to override if needed)
spark_num_exec=5
spark_exec_core=10
spark_exec_mem=2g
spark_drv_mem=8g
spark_drv_max_res=5g
spark_kryo_buf_max=128m
spark_exec_mem_overhead=1536m
#SPECIFICS
#beeline -u $hive_url -e "create database ${hive_dbname} location '${hdfs_functional_path}/internal_data/${hdfs_foldername}/database';"
beeline -u $hive_url -e "use ${hive_dbname};show tables;"
current_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source ${current_dir}/fctutils4C.sh

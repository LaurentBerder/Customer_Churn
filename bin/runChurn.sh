#!/bin/bash
######################################################################################################################################################
# Application Name     : CHURN
# Description          : Run Churn Python Spark Tasks
# Server               : Edge Nodes
# Original Script Name : exportData4C.sh
# Calling Parameters   : NONE
# Files IN             : NONE
# Files OUT            : NONE
# History              : 2018.05.02 - Edited by Laurent Berder
######################################################################################################################################################

source ${project_path}/bin/var_churn4C.sh

spark_log_file=${fs_logs_path}churn_`date '+%Y.%m.%d'`.log
spark_log_csv=${fs_logs_path}churn_`date '+%Y.%m.%d'`.csv
echo "EndDate;Program;ErrorCode;Duration(s)" >> ${spark_log_csv}

current_date=`date '+%Y.%m.%d_%H:%M:%S'`
echo "`date '+%Y.%m.%d_%H:%M:%S'` - Starting Churn Spark tasks on ${current_date}" | tee -a ${spark_log_file}

# Original config
spark_options=" --files /usr/hdp/current/spark2-client/conf/hive-site.xml --num-executors ${spark_num_exec} --executor-cores ${spark_exec_core}  --executor-memory ${spark_exec_mem}  --driver-memory ${spark_drv_mem} --conf spark.driver.maxResultSize=${spark_drv_max_res} --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.kryoserializer.buffer.max=${spark_kryo_buf_max}"
# spark_options=" --files /usr/hdp/current/spark2-client/conf/hive-site.xml --num-executors 20 --executor-cores 12  --executor-memory 30g  --driver-memory 8g --conf spark.driver.maxResultSize=5g --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.kryoserializer.buffer.max=128m"


# Create zip file of all python files, to be duplicated to all nodes
if [ $dev == "_dev" ]
then
cd ${fs_sources_path}
zip -r churnpyfiles.zip ./*
fi


source ${fs_venv_path}/bin/activate
export PYSPARK_PYTHON=${fs_venv_path}/bin/python2.7
export  pyt_env_opt=" --master yarn --deploy-mode client --py-files churnpyfiles.zip"


# Run spark
SECONDS=0
py_program="churn_python/churn_main.py"
run_date=`date '+%Y-%B-%d'`
$SPARK_HOME/bin/spark-submit $spark_options  $pyt_env_opt ${fs_sources_path}${py_program} \
                            --database ${hive_dbname} --churn_table DD_CUSTATRISK_WKYEAR_V \
                            --min_learning_weeks 30 --date ${run_date}
ret_code=$?
current_date=`date '+%Y.%m.%d_%H:%M:%S'`
echo ${current_date} exit code is ${ret_code} while running ${fs_sources_path}${py_program} - Took $SECONDS seconds | tee -a ${spark_log_file}
echo "${current_date};${py_program};${ret_code};$SECONDS" >> ${spark_log_csv}

deactivate

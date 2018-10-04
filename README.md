# Churn risk classification for partners

The objective of this project is to create alerts for partners which are in risk
of churning.

## Prerequisites
This project was developped on a Hadoop Cluster.

To start the project, make sure to use a virtual environment, and install the
required packages listed in [requirements.txt](https://github.com/LaurentBerder/Customer_Churn/blob/master/requirements.txt)
```
cd {project_path}/tools/
virtualenv -p /usr/bin/python2.7 venv
source tools/venv/bin/activate
pip install -r requirements.txt
```

## Running
To launch the classification, simply run the [.sh file](https://github.com/LaurentBerder/Customer_Churn/blob/master/bin/full_run.sh)
```
cd {project_path}/bin/
source ./full_run.sh
```
This will launch successively:
* var_churn4C.sh (definition of variables needed for the project)
* importData4C.sh (import data from Oracle to Hive)
* runChurn.sh (classification)
* exportData4C.sh (export classification results from Hive to Oracle)

The successive .sh files are only launched if the preceding ones did not fail to avoid exporting wrong data to Oracle.

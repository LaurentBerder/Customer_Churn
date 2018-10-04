# coding=utf-8
from churn_python.churn_runner import ChurnRunner
import argparse
import datetime


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-e', '--env',
                        help='sets execution environment ' +
                             '(default: production)',
                        default='production')
    parser.add_argument('--hive', action='store_true', default=False,
                        dest='hive',
                        help='Use Hive for source data (default False)')
    parser.add_argument('--database', dest='database',
                        help='Hive database to use')
    parser.add_argument('--churn_table', dest='churn_table',
                        help='Churn table')
    parser.add_argument('--min_learning_weeks', dest='min_learning_weeks', type=int,
                        help='minimum learning data to use for classification, in number of weeks')
    parser.add_argument('--date', dest='date', help='The run date to save in Hive table')
    return parser.parse_args()


def main():
    arguments = parse_args()

    churn_runner = ChurnRunner(arguments.hive, arguments.min_learning_weeks, arguments.date)
    classif = churn_runner.run(arguments.database, arguments.churn_table)
    # churn_runner = ChurnRunner(False, 30, date=str(datetime.datetime.now()))
    # classif = churn_runner.run(database='churn_dev', churn_table='dd_custatrisk_bk')
    print("""**********************************************************************************
          \n******************************* CLASSIFICATION ***********************************
          \n**********************************************************************************""")
    print("Length of classification: ", len(classif))

    classif = churn_runner.sql_context.createDataFrame([row for row in classif])
    classif.show()

    # Save classification results in Hive table
    classif.write.mode("append").saveAsTable("churn_risk_classification")


if __name__ == '__main__':
    main()

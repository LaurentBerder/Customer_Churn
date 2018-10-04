# coding=utf-8
from churn_python.churn_data import load_data_from_hive
from churn_python.churn_classify import classification
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
from functools import partial
import warnings
import pandas as pd


class ChurnRunner(object):
    sc = None
    sql_context = None

    def __init__(self, hive, min_learning_weeks, date):
        self.min_learning_weeks = min_learning_weeks
        self.current_date = pd.to_datetime(date, yearfirst=True).date()
        self.hive = hive
        self.sc = SparkContext(conf=SparkConf())
        self.sql_context = HiveContext(self.sc)

    def _run_spark(self, cust_at_risk):
        """
        Filter out partners without enough data (less then self.min_learning_weeks)
        For each partner (identified by partner_code), return their churn risk classification
        :param cust_at_risk: a Pyspark DataFrame with time series of all partners
        :return: list of Row for each partners with their classification
        """
        def _run(data, date, min_date, min_learning_weeks):
            """
            Classify churn risk based on regression slope at requested date.
            :param data: a single partner's historical time series of TEU shipping cumulative over 1 year, in pyspark.sql.DataFrame
            :param date: requested date to calculate risks, as string
            :param min_date: first week of data, as string
            :param min_learning_weeks: minimal number of data required data points
            :return: that partner's classification in churn risk level
            """
            data = [row.asDict() for row in data]
            data = pd.DataFrame(data, columns=data[0].keys())
            if len(data.index) < min_learning_weeks:
                message = 'Not enough data ({} lines) for partner {} ({})'
                warnings.warn(message.format(len(data.index),
                                             data.dcd_fullname.unique()[0],
                                             data.dcd_partner_code.unique()[0]))
            res = classification(data, min_date, date)
            return [res]

        # Filter on partners with sufficient data
        from pyspark.sql.functions import col
        partners = cust_at_risk.groupBy('dcd_partner_code').count()
        partners_enough_data = partners.where(col('count') > self.min_learning_weeks)
        cust_at_risk = cust_at_risk.join(partners_enough_data, "dcd_partner_code")
        min_date = cust_at_risk.agg({"etd_wkdat": "min"}).collect()[0][0]
        print("NUMBER OF PARTNERS:", partners_enough_data.count(),
              "(Skipped", partners.count() - partners_enough_data.count(), "partners without enough data")
        print("FIRST LINE OF INPUT DATA:", cust_at_risk.first())

        # Partition
        out = {}
        count = 0
        for key in cust_at_risk.select("dcd_partner_code").distinct().collect():
            out[key.dcd_partner_code] = count
            count += 1

        # Run classification
        rdd = cust_at_risk.rdd.repartition(500) \
            .groupBy(lambda x: (x.dcd_partner_code, x.dcd_fullname)) \
            .partitionBy(len(out), lambda x: out[x[0]]) \
            .mapValues(partial(_run, date=self.current_date, min_date=min_date, min_learning_weeks=self.min_learning_weeks)) \
            .flatMap(lambda x: x[1], preservesPartitioning=True) \
            .cache()
        print("FIRST 10 LINES OF OUTPUT:", rdd.take(10))

        df = rdd.toDF()
        return df.collect()

    def run(self, database=None, churn_table=None):
        """
        Load data, and run classification
        :param database: string (name of Hive database)
        :param churn_table: string (name of Hive table)
        :return: pyspark.sql.DataFrame with list of partners and their level of churn risk
        """
        if self.hive:
            cust_at_risk = pd.read_csv("data/DD_CUSTATRISK_WKYEAR_V.csv")
            cust_at_risk.rename(columns={col: col.lower() for col in cust_at_risk.columns}, inplace=True)
            cust_at_risk = self.sql_context.createDataFrame(cust_at_risk.dropna())
            return self._run_spark(cust_at_risk)
        else:
            cust_at_risk = load_data_from_hive(self.sql_context, database, churn_table)
            return self._run_spark(cust_at_risk)

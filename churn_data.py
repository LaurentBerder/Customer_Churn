# coding=utf-8
import pandas as pd
import numpy as np
import datetime
from isoweek import Week
from dateutil.relativedelta import relativedelta, MO
from pyspark.sql.functions import udf


#Create a Method to handle the Non Ascii to Ascii conversion
def nonasciitoascii(unicodestring):
    return unicodestring.encode("ascii", "ignore")


def load_data_from_hive(sql_context, database, churn_table):
    """
    Simple load from Hive as "select * from".
    :param sql_context: churn_runner.sql_context
    :param database: name of database as string
    :param churn_table: name of table as string
    :return: pyspark.sql.dataframe.DataFrame
    """
    sql_context.sql("use {}".format(database)).collect()
    cust_at_risk = sql_context.sql("select * from {} where dcd_partner_code is not null".format(churn_table))
    cust_at_risk = cust_at_risk.na.drop()

    # Convert any non-ascii characters to ascii in the dcd_fullname column to avoid errors
    converted_udf = udf(nonasciitoascii)
    cust_at_risk = cust_at_risk.withColumn('dcd_fullname', converted_udf(cust_at_risk.dcd_fullname))

    return cust_at_risk


def prepare_data(partner, min_date, date):
    """
    With a time series as input:
    - remove future bookings
    - apply a threshold to avoid outliers
    - fill in the missing dates with zero volume
    - generate rolling year TEU
    :param partner: pd.DataFrame
    :param min_date: first date in whole data, as string
    :param date: Requested date as a string
    :return: pd.DataFrame
    """
    if 'wk' in partner.columns:
        partner['etd_wkdat'] = [Week(int(str(x)[0:4]), int(str(x)[4:6])).monday() for x in partner.wk]
        partner['etd_week'] = [str(Week.withdate(w)[0])+'-'+'{:02d}'.format(Week.withdate(w)[1]) for w in partner.etd_wkdat]
    else:
        partner.loc[:, 'etd_wkdat'] = pd.to_datetime(partner.etd_wkdat)
    #partner.loc[:, 'extract_date'] = [str(pd.to_datetime(x, yearfirst=True).date()) for x in partner.extract_date]

    # Remove future bookings
    partner = partner[partner.etd_wkdat <= date]

    # Replace outlier values
    partner.weekly_teu = replace_outliers(partner.weekly_teu)

    # Rolling year TEU: Sum of TEU over a moving window of 1 year
    partner['rolling_year_teu'] = generate_rolling_year_teu(partner, date)

    # Generate lines for missing dates with 0 in numerical variables
    partner = missing_dates(partner, min_date, date)

    # Remove 2016 data (only useful for calculating rolling year TEU)
    partner.index = range(1, len(partner.index) + 1)
    partner = partner.loc[partner.etd_wkdat >= datetime.datetime.strptime('1/1/2017', '%d/%m/%Y').date()]

    return partner


def missing_dates(partner, min_date, date):
    first_week = partner.etd_week.min()
    full_dates = [str(p).split('/')[0] for p in
                  pd.period_range(datetime.datetime.strptime(min_date, '%Y-%m-%d'),
                                  max(date + relativedelta(weekday=MO(-1)),
                                      partner.etd_wkdat.max()),
                                  freq='W-SUN')]
    full_weeks = [datetime.datetime.strptime(d, '%Y-%m-%d').strftime("%Y-%W") for d in full_dates]
    for idx, week in enumerate(full_weeks):
        if not partner.etd_week.isin([week]).any():
            if week < first_week:
                rolling_year_teu = 0
            else:
                try:
                    rolling_year_teu = partner[partner.etd_week == full_weeks[idx-1]].rolling_year_teu.values[0] - \
                                       partner[partner.etd_wkdat == (datetime.datetime.strptime(str(int(week.split('-')[0]) - 1) +
                                                                                    '-' + week.split('-')[1] + '-1',
                                                                                    '%Y-%W-%w') -
                                                                     datetime.timedelta(days=7))].weekly_teu.values[0]

                except:
                    rolling_year_teu = partner[partner.etd_week == full_weeks[idx - 1]].rolling_year_teu.values[0]
            partner = partner.append({'dcd_partner_code': partner.dcd_partner_code.unique()[0],
                                      'dcd_fullname': partner.dcd_fullname.unique()[0],
                                      'etd_week': str(week),
                                      'etd_wkdat': datetime.datetime.strptime(full_dates[idx], '%Y-%m-%d'),
                                      #'extract_date': partner.extract_date.unique()[0],
                                      'weekly_teu': 0,
                                      'rolling_year_teu': rolling_year_teu},
                                     ignore_index=True)
    partner = partner.sort_values('etd_week')
    return partner


def replace_outliers(column):
    """
    Replace values exceeding average +/- 2 standard deviations in a column by this threshold
    :param column: pd.Series
    :return: column, the same pd.Series without outliers
    """
    column = column.astype('float')
    avg = column.mean()
    std = column.std()

    column[column > avg + std * 2] = np.round(avg + std * 2)
    column[column < avg - std * 2] = np.round(avg - std * 2)

    return column


def generate_rolling_year_teu(partner, date):
    partner.index = pd.to_datetime(partner.etd_wkdat)

    rolling_year_teu = [
        partner.loc[edt - pd.tseries.offsets.DateOffset(years=1):edt, 'weekly_teu'].sum() for edt in partner.index]
    return rolling_year_teu
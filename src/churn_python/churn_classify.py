from churn_python.churn_data import prepare_data
import numpy as np
import pandas as pd
from pyspark.sql import Row
# from dateutil import parser
from dateutil.relativedelta import relativedelta, MO
import datetime
# import matplotlib.pyplot as plt
# import matplotlib.dates as mdates


def classification(partner, min_date, date):
    """
    For a partner's history of volume, we classify both the long term and short term risks of churn.
    Short term is just the ratio between current volume and previous year's volume at the same period.
    Long term depends on the value of the slope to regression of cumulative history
    Both long and short term risks are classified as boolean.
    :param partner: A partner's shipping TEU time series in a Pandas Dataframe
    :param: min_date: first date of overall data as a string
    :param date: Requested date as a string
    :return: classif, a pyspark.sql.Row
    """
    partner = prepare_data(partner, min_date, date)

    long_term_slope, long_term_evolution, long_term_risk = long_term(partner, date)
    short_term_slope, short_term_evolution, short_term_risk = short_term(partner, date)

    classif = {'partner_name': partner.dcd_fullname.unique()[0],
               'partner_code': partner.dcd_partner_code.unique()[0],
               #'extract_date': str(partner.extract_date.unique()[0]),
               'long_term_slope': long_term_slope,
               'long_term_evolution': long_term_evolution,
               'long_term_risk': long_term_risk,
               'short_term_slope': short_term_slope,
               'short_term_evolution': short_term_evolution,
               'short_term_risk': short_term_risk,
               'run_date': date}

    # Turn dict to a Row for return
    classif = Row(**classif)
    print("CLASSIF OUTPUT DATA: ", classif)

    return classif


def long_term(partner, date):
    """
    Takes a time series dataframe, converts dates to a series of numbers, uses it as x and the
    cumulative 1-year TEU of the partner's shipping history as y for a level 2 polynomial regression.
    We derive this polynome a the required date to find the slope of that function in order to classify
    the risk of that partner to churn.
    :param partner: pd.DataFrame
    :param date: Requested date as a string
    :return: lt_slope the slope of the derivative to polynomial regression, a float
    """
    lt_risk = False

    # mdate = parser.parse(str(date)).toordinal()
    mdate = partner[partner.etd_wkdat == date].index[0]
    # x = [x.toordinal() for x in partner.etd_wkdat]
    x = partner.index
    y = partner.rolling_year_teu.tolist()
    p2 = np.poly1d(np.polyfit(x, y, 2))
    print(p2)

    lt_slope = p2.deriv()[1] * np.float64(mdate) + p2.deriv()[0]
    print(lt_slope)
    lt_slope = float("{0:.2f}".format(lt_slope))

    # dd = mdates.num2date(x)
    # _ = plt.plot(dd, y, '-', dd, p2(x), '--')
    # plt.ylabel('TEU')
    # plt.xlabel(partner.dcd_fullname.unique()[0])
    # plt.show()

    # lt_evolution: average rolling_year_teu over last 6 weeks compared to the same 6 weeks the previous year
    current_period = partner[partner.etd_wkdat.isin(
        [pd.to_datetime(str(w).split('/')[0]).date() for w in
         pd.period_range(date - datetime.timedelta(weeks=51) + relativedelta(weekday=MO(-1)),
                         date + relativedelta(weekday=MO(-1)), freq='W-SUN')]
    )].rolling_year_teu
    last_year = partner[partner.etd_wkdat.isin(
        [pd.to_datetime(str(w).split('/')[0]).date() for w in
         pd.period_range(date - datetime.timedelta(days=365.25) - datetime.timedelta(weeks=51)
                         + relativedelta(weekday=MO(-1)),
                         date - datetime.timedelta(days=365.25)
                         + relativedelta(weekday=MO(-1)), freq='W-SUN')]
    )].rolling_year_teu

    lt_evolution = float("{0:.2f}".format(
        (current_period.mean() - last_year.mean()) / last_year.mean()))
    # Handle non-numerical results (replace them with 100% increase because they represent a division by zero
    # for the case of past = some number present = zero)
    lt_evolution = 1.0 if np.isinf(lt_evolution) or np.isnan(lt_evolution) else lt_evolution

    if lt_slope < -1.0 or lt_evolution < -0.3:
        lt_risk = True

    return lt_slope, lt_evolution, lt_risk


def short_term(partner, date):
    """
    Looks at a period of 12 weeks does a linear regression of the cumulative 1-year TEU.
    If the slope is negative and inferior to the slope of the linear regression
    over the same period of the previous year, the partner is classified at risk.
    :param partner: pd.DataFrame
    :param date: Requested date as a string
    :return: st_slope, a float
    """
    st_risk = False

    # Current period: 12 previous and current weeks
    current_period = partner[partner.etd_wkdat.isin(
        [pd.to_datetime(str(w).split('/')[0]).date() for w in
         pd.period_range(date - datetime.timedelta(weeks=11) + relativedelta(weekday=MO(-1)),
                         date + relativedelta(weekday=MO(-1)), freq='W-SUN')]
                                                    )]
    # x_curr = [x.toordinal() for x in current_period.etd_wkdat]
    x_curr = current_period.index
    y_curr = current_period.rolling_year_teu.tolist()

    p_curr = np.poly1d(np.polyfit(x_curr, y_curr, 1))
    print(p_curr)
    st_slope = float("{0:.2f}".format(p_curr.deriv()[0]))

    # Last year period: same 12 weeks, one year prior
    last_year = partner[partner.etd_wkdat.isin(
        [pd.to_datetime(str(w).split('/')[0]).date() for w in
         pd.period_range(date - datetime.timedelta(days=365.25) - datetime.timedelta(weeks=11)
                                                + relativedelta(weekday=MO(-1)),
                         date - datetime.timedelta(days=365.25)
                                                + relativedelta(weekday=MO(-1)), freq='W-SUN')]
                                                )]

    # x_past = [x.toordinal() for x in last_year.etd_wkdat]
    x_past = last_year.index
    y_past = last_year.weekly_teu.tolist()

    p_past = np.poly1d(np.polyfit(x_past, y_past, 1))


    st_evolution = float("{0:.2f}".format(
        (current_period.weekly_teu.mean() - last_year.weekly_teu.mean()) / last_year.weekly_teu.mean()))
    # Handle non-numerical results (replace them with 100% increase because they represent a division by zero
    # for the case of past = some number present = zero)
    st_evolution = 1.0 if np.isinf(st_evolution) or np.isnan(st_evolution) else st_evolution

    if st_slope < p_past.deriv()[0] or st_evolution < -0.3:
        st_risk = True

    return st_slope, st_evolution, st_risk

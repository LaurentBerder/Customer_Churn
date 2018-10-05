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

## Principles
After data import and preparation, risk is assessed at partner level, on two terms: 
- longterm churn risk 
- shortterm churn risk

### Data preparation
Data preparation takes the following steps:

- **Filter data by partner**, grouping shipment volumes by **weekly sums of TEUs**. All the rest of the process is done at this level. 
- **Remove future bookings** as they tend to have a downward influence over the time series.
- Compute a **1-year moving window sum of TEUs** instead of raw figures (e.g. : for week 2018-7, compute sum of TEU for weeks 2017-8 to 2018-7 included) to emphasize trends.
- Determine upper and lower **outlier thresholds**: any value above ![mean + 2 * standard deviation](https://github.com/LaurentBerder/Customer_Churn/blob/master/Support%20images/mean+2std.png "upper threshold") or beneath ![mean - 2 * standard deviation](https://github.com/LaurentBerder/Customer_Churn/blob/master/Support%20images/mean-2std.png "lower threshold") will be replaced by the corresponding threshold.
- **Fill in the blanks of the time series**: data is missing for weeks where no shipment was made, but we instead need empty rows which we generate, filling relevant fields with 0.
- **Remove the first year of data**, since the 1-year moving window sum is not complete.

### Longterm churn risk
The longterm churn risk is based on a polynomial regression (2nd degree) of the partner's shipping volume time series as _y_ and weeks as _x_ (see [Prinples of a polynomial regression](#principles-of-a-polynomial-regression) below).

This returns a curve that follows relatively closely to the observed time series, and the idea is to study this curve's slope on the day we run the algorithm to find out if the partner is on an upward or downward slope.

Therefore, we save the slope, which is the derivative of the curve at the selected date (![derivative](https://github.com/LaurentBerder/Customer_Churn/blob/master/Support%20images/derivative.png "derivative")) as `long_term_slope`.

The other long term indicator that we save as `long_term_evolution` is the ratio between the average TEU sum over the current year and the average of the previous year: ![yearly_ratio](https://github.com/LaurentBerder/Customer_Churn/blob/master/Support%20images/yearly_ratio.png "(mean Y - mean Y-1) / mean Y-1")

#### Principles of a polynomial regression
Find the coefficients ![p[0]](https://github.com/LaurentBerder/Customer_Churn/blob/master/Support%20images/p0.png "p0"),
 ![p[1]](https://github.com/LaurentBerder/Customer_Churn/blob/master/Support%20images/p1.png "p1") and ![p[2]](https://github.com/LaurentBerder/Customer_Churn/blob/master/Support%20images/p2.png "p2")
 such as ![f(x)=p[0]+p[1]*x+p[2]*x²](https://github.com/LaurentBerder/Customer_Churn/blob/master/Support%20images/polynomial_regression.png "polynomial regression equation") 
 is the curve with the lowest error compared to the observed points of the time series ![y](https://github.com/LaurentBerder/Customer_Churn/blob/master/Support%20images/y.png "y").

The error is defined as the sum of squared differences between ![f(x)](https://github.com/LaurentBerder/Customer_Churn/blob/master/Support%20images/f_of_x.png "f(x)"). and ![y](https://github.com/LaurentBerder/Customer_Churn/blob/master/Support%20images/y.png "y"):

![error sum](https://github.com/LaurentBerder/Customer_Churn/blob/master/Support%20images/error_sum.png "error sum")

### Shortterm churn risk
The shortterm churn risk is based on a linear regression (polynomial of the 1st degree) of the partner's shipping volume over the last 12 weeks.

This returns a straight line pointing at the direction the partner is taking at just the latest period. This direction (`short_term_slope`) is compared to the same period in the previous year (in order to take seasonality into account), called `last_year_slope`.

The other short term indicator that we save as `long_term_evolution` is the ratio between the average TEU sum over the current year and the average of the previous year: ![weeks_ratio](https://github.com/LaurentBerder/Customer_Churn/blob/master/Support%20images/weeks_ratio.png "(mean 12 weeks Y - mean 12 weeks Y-1) / mean 12 weeks Y-1")

### Threshold and classification
Once these 4 indicators are computed, we can apply thresholds (defined by the business), of which you can see an example below:

```
If(short_term_slope < 0) or (short_term_slope < last_year_slope) or (short_term_evolution < -0.3):
    short_term_risk = True
Else:
    short_term_risk = False
    
If (long_term_slope < -1) or (long_term_evolution < -0.3):
    long_term_risk = True
Else:
    long_term_risk = False
```

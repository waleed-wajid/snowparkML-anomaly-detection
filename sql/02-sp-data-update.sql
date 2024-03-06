use role SYSADMIN;
use warehouse DEV;

use database DEV_ML_WALEED;
use schema RAW;

create or replace procedure sp_update_data(input_tbl VARCHAR)
  returns string
  language python
  runtime_version = '3.11'
  packages = ('snowflake-snowpark-python', 'pandas')
  handler = 'run'
as
$$
import pandas as pd
import numpy as np


def sample_ts_data(start, end, freq='H', outlier_frac=0.05, random_state=2024):
    """
    this functions returns a dataframe with three columns: EVENT_TIME, EVENT_VALUE and IS_ANOMALY

    Parameters:
    ------------------
    start: datetime for the start of EVENT_TIME
    end: datetime for the end of EVENT_TIME
    freq: frequency of EVENT_TIME
    outlier_frac: proportion of the data to add as noise
    """
    RANDOM_STATE = random_state

    date_range = pd.date_range(start=start, end=end, freq=freq)

    # Create some fake data
    data = np.random.normal(loc=10, scale=1, size=len(date_range))

    # Create a dataframe and add fake data
    col = 'event_value'
    df = pd.DataFrame(index=[d.to_pydatetime() for d in date_range], columns=[col])
    df[col] = data

    
    # add some anomalies to the data
    n_outliers = int(len(date_range) * outlier_frac)
    # print(n_outliers)

    out_idx = df.sample(n_outliers, random_state=RANDOM_STATE).index

    outliers = np.random.normal(loc=20, scale=2, size=n_outliers)

    df.loc[out_idx, col] = outliers

    # using isolation forest notation of 1 for inliers, -1 for outliers
    df['is_anomaly'] = 1
    df.loc[out_idx, 'is_anomaly'] = -1

    df = df.reset_index().rename(columns={'index': 'event_time'})

    df.columns = [col.upper() for col in df.columns]
    return df

def run(session, input_tbl):
    """
    function that generates sample data and appends to the 'input_tbl'

    session: snowpark session object
    input_tbl (str): name of table to upload data to
    """

    last = session.sql(f"select max(EVENT_TIME) from {input_tbl}").collect()[0].as_dict()['MAX(EVENT_TIME)']
    now = session.sql("select current_timestamp()").collect()[0].as_dict()['CURRENT_TIMESTAMP()']

    # add interval
    last = last + pd.Timedelta(hours=1)

    last = last.strftime("%Y-%m-%d %H:%M:%S")
    now = now.strftime("%Y-%m-%d %H:%M:%S")

    df = sample_ts_data(last,now)

    if len(df) > 0:
        session.write_pandas(df, input_tbl, auto_create_table=False,
                             overwrite=False, use_logical_type=True)

        return "SUCCESS: data uploaded"
    else:
        return "Success: no new data to upload"
$$;
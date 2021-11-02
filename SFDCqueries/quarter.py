from datetime import date, datetime, timedelta
today = date.today()
from fiscalyear import FiscalDate
import fiscalyear as fy

import pandas as pd
import datetime as dt


today = date.today()

sr = pd.Series(['2012-10-21', '2019-7-18', '2008-02-2', '2010-4-22', '2019-11-8'])
sr = pd.to_datetime(sr)

cq = sr.dt.quarter

 
current_date = datetime.now()
current_quarter = round((today.month - 1) / 3 + 1)
first_date = datetime(current_date.year, 3 * current_quarter - 2, 1)
last_date = datetime(current_date.year, 3 * current_quarter + 1, 1)\
    + timedelta(days=-1)

print(current_quarter)

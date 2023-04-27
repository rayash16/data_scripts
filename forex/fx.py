#!/usr/bin/env python3

import pandas as pd
from pandas_gbq import to_gbq

# Get credentials via GCP Workload Identity
import google.auth
credentials, project = google.auth.default()

data = pd.read_csv('https://www.rba.gov.au/statistics/tables/csv/f11.1-data.csv', header=None)
new_header = data.iloc[5] # get row header
df = data.iloc[-1:]  # get the last row for the latest
df.columns = new_header
df = df.rename(columns={'Units': 'FX_Date'})
df.set_index('FX_Date')
df = pd.melt(df.reset_index(), id_vars='FX_Date', value_vars=['USD','Index','CNY','JPY','EUR','KRW','GBP','SGD','INR','THB','NZD','TWD','MYR','IDR','VND','AED','PGK','HKD','CAD','ZAR','CHF','PHP','SDR'])
df.rename(columns={5: 'Currency', 'value': 'Rate'}, inplace=True)
final_data = df.iloc[[0,10]] # just use USD, NZD for now - can add more later

projectid = 'lp-core-analytics-prd'
dataset_table = 'lp-core-analytics-prd.supplemental_data.forex'
print("Writing to BigQuery table lp-core-analytics-prd.supplemental_data.forex")
to_gbq(final_data, dataset_table, projectid, if_exists='append', credentials=credentials)
print("Done")

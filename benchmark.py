import os
import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import google.auth
from google.cloud import bigquery
from google.oauth2 import service_account

from pandas_gbq import to_gbq

from googleapiclient.discovery import build


import pytrends
from pytrends.request import TrendReq
import json

import ssl

ssl._create_default_https_context = ssl._create_unverified_context


def download_abs_xls(url):
    return pd.read_excel(url, sheet_name="Data1", header=None, engine="openpyxl")


abs_retail_url = "https://www.abs.gov.au/statistics/industry/retail-and-wholesale-trade/retail-trade-australia/feb-2023/850101.xlsx"
abs_retail_raw = download_abs_xls(abs_retail_url)

# Extract headers
headers = abs_retail_raw.iloc[0, 1:8].tolist()

# Extract data starting from row 11 and select columns A to H
data = abs_retail_raw.iloc[10:, :8]
data.columns = ["Date"] + headers

# Melt data to long format
melted_data = data.melt(id_vars=["Date"], var_name="Industry", value_name="Index")

# Remove unnecessary parts from the Industry column
melted_data["Industry"] = melted_data["Industry"].apply(
    lambda x: x.split(";")[-2].strip() if isinstance(x, str) else x
)

# Convert Date column to datetime format
melted_data["Date"] = pd.to_datetime(melted_data["Date"], format="%b-%Y")


melted_data.iloc[:, 0] = pd.to_datetime(melted_data.iloc[:, 0])

# select rows where the value in the second column is 'Total (Industry)'
# and the date in the first column is from January 2021 onwards
mask = (melted_data.iloc[:, 0] >= "2021-07-01") & (
    melted_data.iloc[:, 1] == "Total (Industry)"
)
abs_retail_data = melted_data.loc[mask]


# CPI and Inflation data
data = pd.read_excel(
    "https://www.rba.gov.au/statistics/tables/xls/g01hist.xls",
    sheet_name="Data",
    header=None,
)

h_rows = data.iloc[[1], [0, 1, 2]]
d_rows = data.iloc[408:, [0, 1, 2]]
r = [h_rows, d_rows]
df = pd.concat(r)
new_header = df.iloc[0]
df = df[1:]
df.columns = new_header
df = df.rename(
    columns={
        "Title": "Date",
        "Consumer price index": "CPI",
        "Year-ended inflation": "Inflation",
    }
)
cpi_data = df
cpi_data["CPI"] = cpi_data["CPI"].fillna(cpi_data["CPI"].mean())
cpi_data["Inflation"] = cpi_data["Inflation"].fillna(cpi_data["Inflation"].mean())


# # Convert date columns to a common format (YYYY-MM-DD)
abs_retail_data["Date"] = pd.to_datetime(abs_retail_data["Date"])

# cpi_data["Date"] = (
#     pd.to_datetime(cpi_data["Date"]).dt.to_period("Q").astype("datetime64[ns]")
# )
cpi_data["Date"] = pd.to_datetime(cpi_data["Date"])


# trends from google - au
def get_google_trends_data_au(keyword):
    # Get the current date and calculate the start and end dates for the timeframe
    current_date = pd.Timestamp.now()
    end_date = current_date - pd.DateOffset(
        months=1
    )  # Subtract 1 month to get the last complete month
    end_date = end_date.replace(day=1) - pd.DateOffset(
        days=1
    )  # Set the end date to the last day of the previous month
    start_date = end_date.replace(
        day=1, year=2021
    )  # Set the start date to the first day of the month in 2021

    timeframe = f"{start_date.strftime('%Y-%m-%d')} {end_date.strftime('%Y-%m-%d')}"

    pytrends = TrendReq(hl="en-US", tz=360)
    pytrends.build_payload([keyword], cat=0, timeframe=timeframe, geo="AU", gprop="")
    data = pytrends.interest_over_time()
    data.reset_index(level=0, inplace=True)
    data = data.rename(columns={"date": "transaction_month", keyword: "google_trends"})

    # Resample the data to a monthly frequency
    data.set_index("transaction_month", inplace=True)
    data = data.resample("M").sum()
    data.reset_index(level=0, inplace=True)

    data["transaction_month"] = data["transaction_month"].dt.to_period("M")
    return data[["transaction_month", "google_trends"]]


google_trends_data_retail_au = get_google_trends_data_au("retail")
google_trends_data_BNPL_au = get_google_trends_data_au("BNPL")

# trends from google - us


def get_google_trends_data_us(keyword):
    # Get the current date and calculate the start and end dates for the timeframe
    current_date = pd.Timestamp.now()
    end_date = current_date - pd.DateOffset(
        months=1
    )  # Subtract 1 month to get the last complete month
    end_date = end_date.replace(day=1) - pd.DateOffset(
        days=1
    )  # Set the end date to the last day of the previous month
    start_date = end_date.replace(
        day=1, year=2021
    )  # Set the start date to the first day of the month in 2021

    timeframe = f"{start_date.strftime('%Y-%m-%d')} {end_date.strftime('%Y-%m-%d')}"

    pytrends = TrendReq(hl="en-US", tz=360)
    pytrends.build_payload([keyword], cat=0, timeframe=timeframe, geo="US", gprop="")
    data = pytrends.interest_over_time()
    data.reset_index(level=0, inplace=True)
    data = data.rename(columns={"date": "transaction_month", keyword: "google_trends"})

    # Resample the data to a monthly frequency
    data.set_index("transaction_month", inplace=True)
    data = data.resample("M").sum()
    data.reset_index(level=0, inplace=True)

    data["transaction_month"] = data["transaction_month"].dt.to_period("M")
    return data[["transaction_month", "google_trends"]]


google_trends_data_retail_us = get_google_trends_data_us("retail")
google_trends_data_BNPL_us = get_google_trends_data_us("BNPL")


# trends from google - eu
def get_google_trends_data_eu(keyword):
    # Get the current date and calculate the start and end dates for the timeframe
    current_date = pd.Timestamp.now()
    end_date = current_date - pd.DateOffset(
        months=1
    )  # Subtract 1 month to get the last complete month
    end_date = end_date.replace(day=1) - pd.DateOffset(
        days=1
    )  # Set the end date to the last day of the previous month
    start_date = end_date.replace(
        day=1, year=2021
    )  # Set the start date to the first day of the month in 2021

    timeframe = f"{start_date.strftime('%Y-%m-%d')} {end_date.strftime('%Y-%m-%d')}"

    pytrends = TrendReq(hl="en-US", tz=360)
    pytrends.build_payload([keyword], cat=0, timeframe=timeframe, geo="GB", gprop="")
    data = pytrends.interest_over_time()
    data.reset_index(level=0, inplace=True)
    data = data.rename(columns={"date": "transaction_month", keyword: "google_trends"})

    # Resample the data to a monthly frequency
    data.set_index("transaction_month", inplace=True)
    data = data.resample("M").sum()
    data.reset_index(level=0, inplace=True)

    data["transaction_month"] = data["transaction_month"].dt.to_period("M")
    return data[["transaction_month", "google_trends"]]


google_trends_data_retail_eu = get_google_trends_data_eu("retail")
google_trends_data_BNPL_eu = get_google_trends_data_eu("BNPL")
# Rename the columns to avoid duplicated column names
google_trends_data_retail_au.rename(
    columns={"google_trends": "google_trends_retail_au"}, inplace=True
)
google_trends_data_BNPL_au.rename(
    columns={"google_trends": "google_trends_BNPL_au"}, inplace=True
)
google_trends_data_retail_us.rename(
    columns={"google_trends": "google_trends_retail_us"}, inplace=True
)
google_trends_data_BNPL_us.rename(
    columns={"google_trends": "google_trends_BNPL_us"}, inplace=True
)
google_trends_data_retail_eu.rename(
    columns={"google_trends": "google_trends_retail_eu"}, inplace=True
)
google_trends_data_BNPL_eu.rename(
    columns={"google_trends": "google_trends_BNPL_eu"}, inplace=True
)

google_trends_data_retail_au["transaction_month"] = google_trends_data_retail_au[
    "transaction_month"
].apply(lambda x: x.to_timestamp())
google_trends_data_BNPL_au["transaction_month"] = google_trends_data_BNPL_au[
    "transaction_month"
].apply(lambda x: x.to_timestamp())
google_trends_data_retail_us["transaction_month"] = google_trends_data_retail_us[
    "transaction_month"
].apply(lambda x: x.to_timestamp())
google_trends_data_BNPL_us["transaction_month"] = google_trends_data_BNPL_us[
    "transaction_month"
].apply(lambda x: x.to_timestamp())
google_trends_data_retail_eu["transaction_month"] = google_trends_data_retail_eu[
    "transaction_month"
].apply(lambda x: x.to_timestamp())
google_trends_data_BNPL_eu["transaction_month"] = google_trends_data_BNPL_eu[
    "transaction_month"
].apply(lambda x: x.to_timestamp())


os.environ["GOOGLE_CLOUD_PROJECT"] = "lp-core-analytics-prd"
project_id = os.environ["GOOGLE_CLOUD_PROJECT"]
credentials, project = google.auth.default()
client = bigquery.Client(project=project_id)

query = """ /*Useing quarter_end to match cpi data*/
SELECT date_trunc(transaction_date,month) as transaction_month, 
DATE_SUB(DATE_ADD(DATE_TRUNC(date(transaction_date), QUARTER), INTERVAL 3 MONTH), INTERVAL 1 DAY) as transaction_quarter
,transaction_year, sum(transaction_amount) as sales
FROM `lp-core-analytics-prd.analytics_datamart.view_selfservice`
WHERE date_trunc(transaction_date,month) >'2021-06-01' 
AND NumberofSuccessfullTransactions=1
GROUP BY 1,2,3

"""
company_data = client.query(query).to_dataframe()

query = """
SELECT date_trunc(transaction_date,month) as transaction_month, sum(transaction_amount) as sales_bnpl
FROM `lp-core-analytics-prd.analytics_datamart.view_selfservice`
WHERE date_trunc(transaction_date,month) >'2021-06-01' 
AND NumberofSuccessfullTransactions=1
and is_pay_plan=1
group by 1
"""
company_data_BNPL = client.query(query).to_dataframe()


company_data["transaction_month"] = (
    pd.to_datetime(company_data["transaction_month"])
    .dt.to_period("M")
    .dt.to_timestamp()
)
company_data["transaction_quarter"] = pd.to_datetime(
    company_data["transaction_quarter"]
)

company_data_BNPL["transaction_month"] = (
    pd.to_datetime(company_data_BNPL["transaction_month"])
    .dt.to_period("M")
    .dt.to_timestamp()
)

print(company_data.head(5))

# Create month and quarter columns in external datasets
abs_retail_data["quarter"] = abs_retail_data["Date"].dt.to_period("Q").dt.to_timestamp()
# cpi_data["month"] = cpi_data["Date"].dt.to_period("M").dt.to_timestamp()
# google_trends_data_retail_au["month"] = google_trends_data_retail_au["transaction_month"]

# Rename the date columns in external datasets to match the company data columns
abs_retail_data.rename(columns={"Date": "transaction_month"}, inplace=True)
cpi_data.rename(columns={"Date": "transaction_quarter"}, inplace=True)
# google_trends_data.rename(columns={"transaction_month": "month"}, inplace=True)
print(cpi_data.head(5))

# Merge company data with external datasets
merged_data = company_data.merge(abs_retail_data, on="transaction_month", how="left")
merged_data = merged_data.merge(company_data_BNPL, on="transaction_month", how="left")
merged_data = merged_data.merge(cpi_data, on="transaction_quarter", how="left")
merged_data = merged_data.merge(
    google_trends_data_retail_au, on="transaction_month", how="left"
)
merged_data = merged_data.merge(
    google_trends_data_BNPL_au,
    on="transaction_month",
    how="left",
    suffixes=("", "_BNPL_au"),
)
merged_data = merged_data.merge(
    google_trends_data_retail_us,
    on="transaction_month",
    how="left",
    suffixes=("", "_retail_us"),
)

merged_data = merged_data.merge(
    google_trends_data_BNPL_us,
    on="transaction_month",
    how="left",
    suffixes=("", "_BNPL_us"),
)
merged_data = merged_data.merge(
    google_trends_data_retail_eu,
    on="transaction_month",
    how="left",
    suffixes=("", "_retail_eu"),
)
merged_data = merged_data.merge(
    google_trends_data_BNPL_eu,
    on="transaction_month",
    how="left",
    suffixes=("", "_BNPL_eu"),
)


# Rename the columns to make them easier to understand
merged_data.rename(columns={"Index": "abs_retail_index"}, inplace=True)


def write_dataframe_to_bigquery(df, table_id, project_id, credentials):
    client = bigquery.Client(project=project_id, credentials=credentials)
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=[
            bigquery.SchemaField("transaction_month", "DATE"),
            bigquery.SchemaField("transaction_quarter", "DATE"),
            bigquery.SchemaField("transaction_year", "INT64"),
            bigquery.SchemaField("Industry", "STRING"),
            bigquery.SchemaField("sales", "FLOAT64"),
            # Add schema fields for the merged datasets
            bigquery.SchemaField("abs_retail_index", "FLOAT64"),
            bigquery.SchemaField("CPI", "FLOAT64"),
            bigquery.SchemaField("Inflation", "FLOAT64"),
            bigquery.SchemaField("google_trends_retail_au", "INT64"),
            bigquery.SchemaField("google_trends_BNPL_au", "INT64"),
            bigquery.SchemaField("google_trends_retail_us", "INT64"),
            bigquery.SchemaField("google_trends_BNPL_us", "INT64"),
            bigquery.SchemaField("google_trends_retail_eu", "INT64"),
            bigquery.SchemaField("google_trends_BNPL_eu", "INT64"),
            bigquery.SchemaField("sales_bnpl", "FLOAT64"),
        ],
    )

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()


# select specific columns by name
merged_data = merged_data.loc[
    :,
    [
        "transaction_month",
        "transaction_quarter",
        "transaction_year",
        "Industry",
        "sales",
        "abs_retail_index",
        "CPI",
        "Inflation",
        "google_trends_retail_au",
        "google_trends_BNPL_au",
        "google_trends_retail_us",
        "google_trends_BNPL_us",
        "google_trends_retail_eu",
        "google_trends_BNPL_eu",
        "sales_bnpl",
    ],
]

table_id = f"{project_id}.analytics_datamart.sales_and_benchmarks"
write_dataframe_to_bigquery(merged_data, table_id, project_id, credentials)
print(f"Data from DataFrame has been written to BigQuery table '{table_id}'.")

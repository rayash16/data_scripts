import os
import pandas as pd
import numpy as np
import re
from pandas_gbq import to_gbq
from google.cloud import bigquery
from google.oauth2 import service_account
from googleapiclient.discovery import build

project_id = "lp-core-analytics-prd"
spreadsheet_id = "1sZtQJhahkcwcSF_yeB2kg41LZMzttLATxT-Wqu4VeJY"
range_name = "Total!A1:V"
dataset_id = "analytics_datamart"  # Replace with your actual dataset ID
bigquery_table_name = f"{dataset_id}.debtcollectionstats"


def clean_column_names(column_name):
    return re.sub(r"[^a-zA-Z0-9]+", "_", column_name)


# Replace with the actual path to your Google Sheets service account JSON key file
sheets_key_file = "/Users/ashishray/source/Service Accounts/ashish-ray-sheets@lp-core-analytics-prd.iam.gserviceaccount.com.json"
# Replace with the actual path to your BigQuery service account JSON key file
bigquery_key_file = (
    "/Users/ashishray/source/Service Accounts/application_default_credentials.json"
)

# Load credentials for Google Sheets
sheets_scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
sheets_credentials = service_account.Credentials.from_service_account_file(
    sheets_key_file, scopes=sheets_scopes
)

# Load credentials for BigQuery
bigquery_scopes = ["https://www.googleapis.com/auth/bigquery"]
bigquery_credentials = service_account.Credentials.from_service_account_file(
    bigquery_key_file, scopes=bigquery_scopes
)

# Construct a BigQuery client object.
client = bigquery.Client(project=project_id, credentials=bigquery_credentials)

# Access Google Sheets API
sheets_service = build("sheets", "v4", credentials=sheets_credentials)
result = (
    sheets_service.spreadsheets()
    .values()
    .get(spreadsheetId=spreadsheet_id, range=range_name)
    .execute()
)

try:
    rows = result.get("values", [])
    headers = rows.pop(0)
    df = pd.DataFrame(rows, columns=headers)
except Exception as error:
    print(f"An error occurred: {error}")
    df = None

if df is not None:
    print(
        f"Data from Google Sheet has been read into a DataFrame with {len(df)} rows and {len(df.columns)} columns."
    )

    # Clean the column names in the DataFrame
    df.columns = [clean_column_names(col) for col in df.columns]

    # Upload the DataFrame to BigQuery
    to_gbq(
        df,
        bigquery_table_name,
        project_id,
        if_exists="replace",
        credentials=bigquery_credentials,
    )
    print(
        f"Data from DataFrame has been written to BigQuery table '{bigquery_table_name}'."
    )
else:
    print("No data found.")

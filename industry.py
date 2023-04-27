import os
import pandas as pd
import numpy as np
import re
from pandas_gbq import to_gbq
from google.cloud import bigquery
from google.oauth2 import service_account
from googleapiclient.discovery import build

project_id = "lp-core-analytics-prd"
spreadsheet_id = "1g4kHx2jNqSVoCNeO6RdVxGmvEtoFyjlWwv0z6wqcBBY"
range_name = "AU Merchant list!C4:C,G4:G,L4:L"
dataset_id = "analytics_datamart"  # Replace with your actual dataset ID
bigquery_table_name = f"{dataset_id}.industry"


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
range_list = ["AU Merchant list!C4:C", "AU Merchant list!G4:G", "AU Merchant list!L4:L"]
headers = ["brand_name", "merchant_id", "Industry"]

try:
    data = []

    for rng in range_list:
        result = (
            sheets_service.spreadsheets()
            .values()
            .get(spreadsheetId=spreadsheet_id, range=rng)
            .execute()
        )
        column_data = result.get("values", [])
        cleaned_column_data = [
            cell[0] if cell else "" for cell in column_data
        ]  # Extract the first item or use an empty string
        data.append(cleaned_column_data)

    rows = list(zip(*data))
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

    # Convert columns to string type
    for col in df.columns:
        df[col] = df[col].astype(str)

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

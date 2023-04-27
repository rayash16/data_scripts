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
range_name_saas_pipeline_channel = "SaaS Pipeline Channel!A1:S14"
range_name_stack_pipeline = "STACK_Pipeline!A2:T34"
range_name_saas_pipeline_partnership = "SAAS AND PARTNERSHIPS PIPELINE!A2:S9"

dataset_id = "analytics_datamart"
bigquery_table_name_saas = f"{dataset_id}.saas_pipeline"
bigquery_table_name_stack = f"{dataset_id}.stack_pipeline"


def clean_column_names(column_name):
    return re.sub(r"[^a-zA-Z0-9]+", "_", column_name)


sheets_key_file = "/Users/ashishray/source/Service Accounts/ashish-ray-sheets@lp-core-analytics-prd.iam.gserviceaccount.com.json"
bigquery_key_file = (
    "/Users/ashishray/source/Service Accounts/application_default_credentials.json"
)

sheets_scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
sheets_credentials = service_account.Credentials.from_service_account_file(
    sheets_key_file, scopes=sheets_scopes
)

bigquery_scopes = ["https://www.googleapis.com/auth/bigquery"]
bigquery_credentials = service_account.Credentials.from_service_account_file(
    bigquery_key_file, scopes=bigquery_scopes
)

client = bigquery.Client(project=project_id, credentials=bigquery_credentials)
sheets_service = build("sheets", "v4", credentials=sheets_credentials)


# Function to get data from a sheet
def get_data_from_sheet(range_name):
    result = (
        sheets_service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=range_name)
        .execute()
    )
    return result.get("values", [])


# Fetch data from the sheets
data_saas_channel = get_data_from_sheet(range_name_saas_pipeline_channel)
data_saas_partnership = get_data_from_sheet(range_name_saas_pipeline_partnership)
data_stack = get_data_from_sheet(range_name_stack_pipeline)

# Create DataFrames for the SaaS sheets
saas_channel_df = pd.DataFrame(data_saas_channel[1:], columns=data_saas_channel[0])
saas_partnership_df = pd.DataFrame(
    data_saas_partnership[1:], columns=data_saas_partnership[0]
)

# Add the 'source' column to the DataFrames
saas_channel_df["source"] = "channel"
saas_partnership_df["source"] = "partnership"

# Combine the SaaS DataFrames
saas_combined_df = pd.concat([saas_channel_df, saas_partnership_df], ignore_index=True)


# Convert all columns to their best inferred data types
# saas_combined_df = saas_combined_df.convert_dtypes()


# # Combine the SaaS sheets and add a new column
# saas_combined_data = data_saas_channel + data_saas_partnership
# saas_combined_df = pd.DataFrame(saas_combined_data, columns=data_saas_channel[0])
print(saas_combined_df.columns)

# Create a DataFrame for the STACK_Pipeline data
stack_df = pd.DataFrame(data_stack[1:], columns=data_stack[0])
# stack_df = stack_df.convert_dtypes()


# # Convert specific columns to numeric data types
# numeric_columns = [
#     "Likelihood % ",
#     "Gross Rev Est. (Annual)",
#     "Prob Rev (Annual)",
#     "Prob Rev (Monthly)",
#     "Trans Margin (%)",
#     "Net Rev (monthly)",
# ]
# for column in numeric_columns:
#     saas_combined_df[column] = pd.to_numeric(saas_combined_df[column], errors="coerce")
#     # stack_df[column] = pd.to_numeric(stack_df[column], errors="coerce")


# Clean the column names of the DataFrames
saas_combined_df.columns = saas_combined_df.columns.map(clean_column_names)
stack_df.columns = stack_df.columns.map(clean_column_names)

# Upload the DataFrames to their respective BigQuery tables
to_gbq(
    saas_combined_df,
    bigquery_table_name_saas,
    project_id,
    if_exists="replace",
    credentials=bigquery_credentials,
)
print(
    f"Data from 'SaaS Pipeline' sheets has been written to BigQuery table '{bigquery_table_name_saas}'."
)

to_gbq(
    stack_df,
    bigquery_table_name_stack,
    project_id,
    if_exists="replace",
    credentials=bigquery_credentials,
)
print(
    f"Data from 'STACK_Pipeline' sheet has been written to BigQuery table '{bigquery_table_name_stack}'."
)

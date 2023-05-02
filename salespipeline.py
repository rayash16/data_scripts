import os
import pandas as pd
import numpy as np
import re
from pandas_gbq import to_gbq
from google.cloud import bigquery
from google.oauth2 import service_account
from googleapiclient.discovery import build

project_id = "lp-core-analytics-prd"
spreadsheet_id = "17MEGcJh2TiYk_ttC89JAoB60e-PxnqRBFTKqrgVT3ms"
range_name_saas_pipeline_channel = "SaaS Channel Partner Pipeline!A1:R100"
range_name_stack_pipeline = "STACK_Pipeline!A2:U100"
range_name_saas_pipeline_partnership = "SAAS Direct Pipeline!A2:R100"


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

##get sheeet names
# def get_sheet_names(spreadsheet_id):
#     spreadsheet = (
#         sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
#     )
#     sheets = spreadsheet.get("sheets", [])
#     sheet_names = [sheet.get("properties", {}).get("title") for sheet in sheets]
#     return sheet_names


# # Usage:
# sheet_names = get_sheet_names(spreadsheet_id)


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

# print("Data from SaaS Channel Partner Pipeline sheet:")
# print(data_saas_channel)
# print("\nData from SAAS Direct Pipeline sheet:")
# print(data_saas_partnership)
# print("\nData from STACK_Pipeline sheet:")
# print(data_stack)

# Create DataFrames for the SaaS sheets
saas_channel_df = pd.DataFrame(data_saas_channel[1:], columns=data_saas_channel[0])
saas_partnership_df = pd.DataFrame(
    data_saas_partnership[1:], columns=data_saas_partnership[0]
)

# Add the 'source' column to the DataFrames
saas_channel_df["source"] = "channel"
saas_partnership_df["source"] = "partnership"


# print(saas_channel_df.columns)
# print(saas_channel_df.columns)


# Combine the SaaS DataFrames
saas_combined_df = pd.concat([saas_channel_df, saas_partnership_df], ignore_index=True)


# Create a DataFrame for the STACK_Pipeline data
stack_df = pd.DataFrame(data_stack[1:], columns=data_stack[0])
# stack_df = stack_df.convert_dtypes()

# print(stack_df.columns)
# Clean the column names of the DataFrames
saas_combined_df.columns = saas_combined_df.columns.map(clean_column_names)
stack_df.columns = stack_df.columns.map(clean_column_names)


saas_combined_df = saas_combined_df.drop(
    saas_combined_df[
        (saas_combined_df["Partner"] == "TOTALS")
        | (saas_combined_df.isna().any(axis=1))
    ].index
)

stack_df = stack_df.drop(
    stack_df[
        (stack_df["Direct_STACK_"] == "TOTALS") | (stack_df.isna().any(axis=1))
    ].index
)


numeric_columns_saas = [
    "Likelihood_",
    "Gross_Rev_Est_Annual_",
    "Prob_Rev_Annual_",
    "Prob_Rev_Monthly_",
    "Trans_Margin_",
    "Net_Rev_monthly_",
]

# Numeric columns for stack_df
numeric_columns_stack = [
    "Likelihood_",
    "Annual_TTV",
    "Annual_TTV_Full_",
    "Annual_TTV_BNPL_",
    "Full",
    "BNPL",
    "Est_TTV_Annual_",
    "Prob_TTV_Monthly_",
    "MSF_Full_",
    "MSF_BNPL_",
    "Gross_Rev",
    "Trans_Margin_",
    "Net_Rev_monthly_",
]


def clean_numeric_value(value):
    # Remove unwanted characters
    value = value.replace("$", "").replace(",", "").replace("%", "")

    # Replace any remaining non-numeric characters with NaN
    if not re.match(r"[-+]?\d+(\.\d+)?$", value.strip()):
        return np.nan
    return value


# Convert specific columns to numeric data types

for column in numeric_columns_saas:
    saas_combined_df[column] = saas_combined_df[column].apply(clean_numeric_value)
    saas_combined_df[column] = pd.to_numeric(saas_combined_df[column], errors="coerce")

for column in numeric_columns_stack:
    stack_df[column] = stack_df[column].apply(clean_numeric_value)
    stack_df[column] = pd.to_numeric(stack_df[column], errors="coerce")

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

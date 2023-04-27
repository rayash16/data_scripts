#!/usr/bin/env python3

# NOTE: BQ required perms: roles/bigquery.jobUser (project), roles/bigquery.dataEditor (dataset)

import paramiko
from io import StringIO
import pandas as pd
from pandas_gbq import to_gbq
from google.cloud import bigquery
# Get credentials via GCP Workload Identity
import google.auth
credentials, project = google.auth.default()

host,port = "limepay.sftp.checkout.com",22
transport=paramiko.Transport((host,port))
username= "limepay-sftp-prod"
key="/.ssh/id_rsa" # NOTE: must be mounted as a file
projectid = 'lp-core-analytics-prd'

print("Connecting to limepay.sftp.checkout.com")

with open(key) as f:
    fin=f.read()

final_key=StringIO(fin)
keyFile=paramiko.RSAKey.from_private_key(final_key)

transport=paramiko.Transport((host,port))
transport.connect(username=username,pkey=keyFile)

sftp = paramiko.SFTPClient.from_transport(transport)

print("[Limepay AU] Starting")

sftp.chdir("/limepay-pty-ltd/reports-limepay-pty-ltd/financial-actions/payout-id")
files = sftp.listdir()

df_ftp = pd.DataFrame(files)
df_ftp.columns = ['filename']

sql = "SELECT distinct filename FROM lp-core-analytics-prd.payment_gateway_activity.checkout_payouts_au"

try:
    df_existing = pd.read_gbq(sql, project_id=projectid, dialect='standard', credentials=credentials)
    df_diff = df_ftp.merge(df_existing, on='filename', how='outer', suffixes=['', '_'], indicator=True)
    df = df_diff[df_diff['_merge'] == 'left_only']
except:
    filename = ['test.csv']
    df_existing = pd.DataFrame(filename)
    df_existing.columns = ['filename']
    df_diff = df_ftp.merge(df_existing, on='filename', how='outer', suffixes=['', '_'], indicator=True)
    df = df_diff[df_diff['_merge'] == 'left_only']

filelist = df['filename'].tolist()

print("[Limepay AU] Reading files")
df_list = []
for i in filelist:
    with sftp.open(i) as f:
        f.prefetch()
        df = pd.read_csv(f)
        df['FileName'] = i
        df_list.append(df)
        df= pd.concat(df_list)

df.columns = df.columns.str.replace(' ', '')
df = df.applymap(str)

dataset_table = 'lp-core-analytics-prd.payment_gateway_activity.checkout_payouts_au'
print("[Limepay AU] Writing to BigQuery table checkout_payouts_au")
try:
    to_gbq(df, dataset_table, projectid, if_exists='append', credentials=credentials)
    print("[Limepay AU] Data added")
except:
    print("[Limepay AU] No new data was added")

print("[Limepay AU] Finished")

print("[Limepay NZ] Starting")

sftp.chdir("/limepay-limited/reports-limepay-limited/financial-actions/payout-id")
files = sftp.listdir()

df_ftp = pd.DataFrame(files)
df_ftp.columns = ['filename']

sql = "SELECT distinct filename FROM lp-core-analytics-prd.payment_gateway_activity.checkout_payouts_nz"

try:
    df_existing = pd.read_gbq(sql, project_id=projectid, dialect='standard', credentials=credentials)
    df_diff = df_ftp.merge(df_existing, on='filename', how='outer', suffixes=['', '_'], indicator=True)
    df = df_diff[df_diff['_merge'] == 'left_only']
except:
    filename = ['test.csv']
    df_existing = pd.DataFrame(filename)
    df_existing.columns = ['filename']
    df_diff = df_ftp.merge(df_existing, on='filename', how='outer', suffixes=['', '_'], indicator=True)
    df = df_diff[df_diff['_merge'] == 'left_only']

filelist = df['filename'].tolist()

print("[Limepay NZ] Reading files")
df_list = []
for i in filelist:
    with sftp.open(i) as f:
        f.prefetch()
        df = pd.read_csv(f)
        df['FileName'] = i
        df_list.append(df)
        df= pd.concat(df_list)

df.columns = df.columns.str.replace(' ', '')
df = df.applymap(str)

dataset_table = 'lp-core-analytics-prd.payment_gateway_activity.checkout_payouts_nz'
print("[Limepay NZ] Writing to BigQuery table checkout_payouts_nz")
try:
    to_gbq(df, dataset_table, projectid, if_exists='append', credentials=credentials)
    print("[Limepay NZ] Data added")
except:
    print("[Limepay NZ] No new data was added")

print("[Limepay NZ] Finished")

print("Done")

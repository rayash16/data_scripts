#!/usr/bin/env python3

import json
import os
import pandas
import requests
import stripe
import time
from pandas_gbq import to_gbq
from google.cloud import bigquery
from datetime import datetime, timedelta
from requests.auth import HTTPBasicAuth
from io import StringIO
from pytz import timezone

# Get credentials via GCP Workload Identity
import google.auth
credentials, project = google.auth.default()

# Get api key from env var
stripe.api_key = os.getenv('STRIPE_API_KEY')

# Latest end_days_behind reports are available 4 days behind
# We can override the default 4 days behind via env vars by editing the cronjob and doing a once off manual then, then revert the env vars back to 4
# env vars are strings and must be converted to int
start_days_behind = int(os.getenv('START_DAYS_BEHIND'))
end_days_behind = int(os.getenv('END_DAYS_BEHIND'))

# Use today UTC as date
# NOTE: Add  day=1-31, month=1-12 to replace() if you want another custom date and 4 days before
today_date_start = datetime.now(timezone("UTC")).replace(hour=0, minute=0, second=0, microsecond = 0)
today_date_end = datetime.now(timezone("UTC")).replace(hour=23, minute=59, second=59, microsecond = 999999)

back_date_start = today_date_start - timedelta(days = start_days_behind )
back_date_end = today_date_end - timedelta(days = end_days_behind )

# get the timestamps
back_date_start_ts = int(datetime.timestamp(back_date_start))
back_date_end_ts = int(datetime.timestamp(back_date_end))

print("Start date:", back_date_start, "(", back_date_start_ts, ")")
print("End date:", back_date_end, "(", back_date_end_ts, ")")

print("Creating balance_change_from_activity.itemized.2 report run")
report_run = stripe.reporting.ReportRun.create(
  report_type="balance_change_from_activity.itemized.2",
  parameters={
    "interval_start": back_date_start_ts,
    "interval_end": back_date_end_ts
  },
)

report_run_json = json.loads(str(report_run))
time_waited = 0

while True:
    report_run_details = stripe.reporting.ReportRun.retrieve(report_run_json["id"])
    report_run_details_json = json.loads(str(report_run_details))
    if (report_run_details_json["status"] == "succeeded"):
        break
    print("Retrying in 10s")
    time.sleep(10)
    time_waited = time_waited + 10

print("Report succeeded after", time_waited, "s")
print(report_run_details_json)

csv = requests.get(report_run_details_json["result"]["url"], auth=HTTPBasicAuth(stripe.api_key, ''))

data = pandas.read_csv(StringIO(csv.text), low_memory=False)

projectid = 'lp-core-analytics-prd'
dataset_table = 'lp-core-analytics-prd.payment_gateway_activity.stripe_balance_change_from_activity_itemized_2'

print("Writing to BigQuery table stripe_balance_change_from_activity_itemized_2")

to_gbq(data, dataset_table, projectid, if_exists='append', credentials=credentials)

print("Done")

print("Creating activity.itemized.2 report run")

report_run = stripe.reporting.ReportRun.create(
  report_type="activity.itemized.2",
  parameters={
    "interval_start": back_date_start_ts,
    "interval_end": back_date_end_ts,
    "columns":[
      "balance_transaction_id",
      "balance_transaction_created_at",
      "balance_transaction_reporting_category",
      "balance_transaction_component",
      "activity_at",
      "currency",
      "amount",
      "charge_id",
      "payment_intent_id",
      "refund_id",
      "dispute_id",
      "invoice_id",
      "subscription_id",
      "fee_id",
     "customer_id",
      "customer_email",
      "customer_name",
      "customer_description",
      "shipping_address_line1",
      "shipping_address_line2",
      "shipping_address_city",
      "shipping_address_state",
      "shipping_address_postal_code",
      "shipping_address_country",
      "automatic_payout_id",
      "automatic_payout_effective_at",
      "event_type",
      "payment_method_type",
      "card_brand",
      "card_funding",
      "card_country",
      "statement_descriptor",
      "customer_facing_currency",
      "customer_facing_amount",
      "activity_interval_type",
      "activity_start_date",
      "activity_end_date",
      "balance_transaction_description",
      "connected_account_id",
      "connected_account_name",
      "connected_account_country",
      "connected_account_direct_charge_id",
      "payment_metadata[CustomerEmail]",
      "payment_metadata[paymentType]",
      "payment_metadata[payCardId]",
       "payment_metadata[internalOrderId]",
      "payment_metadata[merchantOrderId]",
      "payment_metadata[payPlanId]",
      "payment_metadata[instalmentNumber]",
      "payment_metadata[payInstalmentId]",
      "payment_metadata[payDeferralId]",
      "payment_metadata[payPlanIdV2]",
      "payment_metadata[merchantId]",
      "payment_metadata[surchargedAmount]",
      "payment_metadata[customerId]",
      "transfer_id",
      "transfer_metadata[borrowerTransitionDate]",
      "transfer_metadata[payCardId]",
       "transfer_metadata[payDeferralId]",
       "transfer_metadata[payPlanId]",
       "transfer_metadata[payPlanIdV2]",
       "transfer_metadata[merchantId]",
       "transfer_metadata[netAmount]",
       "transfer_metadata[reversedAmount]",
       "transfer_metadata[fee]",
       "transfer_metadata[planAmount]",
       "refund_metadata[merchandId]",
       "refund_metadata[payCardID]",
       "refund_metadata[payDeferralId]",
       "refund_metadata[payPlanIdV2]"
      ]
  },
)

report_run_json = json.loads(str(report_run))
time_waited = 0

while True:
    report_run_details = stripe.reporting.ReportRun.retrieve(report_run_json["id"])
    report_run_details_json = json.loads(str(report_run_details))
    if (report_run_details_json["status"] == "succeeded"):
        break
    print("Retrying in 10s")
    time.sleep(10)
    time_waited = time_waited + 10

print("Report succeeded after", time_waited, "s")
print(report_run_details_json)

csv = requests.get(report_run_details_json["result"]["url"], auth=HTTPBasicAuth(stripe.api_key, ''))

data = pandas.read_csv(StringIO(csv.text), low_memory=False)
data.columns = data.columns.str.replace('[', '_', regex=False)
data.columns = data.columns.str.replace(']', '_', regex=False)
data=data.astype(str)

projectid = 'lp-core-analytics-prd'
dataset_table = 'lp-core-analytics-prd.payment_gateway_activity.stripe_activity_itemized'

print("Writing to BigQuery table stripe_activity_itemized_2")
to_gbq(data, dataset_table, projectid, if_exists='append', credentials=credentials)

print("Done")

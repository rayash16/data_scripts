# Stripe report

This script runs as a CronJob daily at 1am UTC on the ops cluster in the `data` namespace.

It fetches the last available daily report `activity.itemized.1` which is 4 days behind, and loads the data into BigQuery table: `lp-core-analytics-prd.payment_gateway_activity.stripe_reports`.

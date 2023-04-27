# CKO payouts

This script runs as a CronJob daily at 1am UTC on the ops cluster in the `data` namespace.

It fetches via sFTP on Checkout.com all CSV payout files, for both Limepay AU and NZ and loads the data into BigQuery tables: `lp-core-analytics-prd.payment_gateway_activity.checkout_payouts_au` & `lp-core-analytics-prd.payment_gateway_activity.checkout_payouts_nz`.

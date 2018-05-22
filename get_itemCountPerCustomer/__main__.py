# to communicate with Google BigQuery
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/indrajitharidas/Downloads/Data Warehouse Production-7bb7eb3af064.json"

credentials = service_account.Credentials.from_service_account_file("/Users/indrajitharidas/Downloads/Data Warehouse Production-7bb7eb3af064.json")
prj = "tetris-effect-203015"

def main():
    client = bigquery.Client(credentials= credentials, project = prj)
    # Set use_legacy_sql to False to use standard SQL syntax.
    # Note that queries run through the Python Client Library are set to use
    # standard SQL by default.
    job_config = bigquery.QueryJobConfig()
    job_config.use_legacy_sql = False
    print('loading data from BigQuery: Cusomer Shopify ID | Item Shopify ID | Count of Item')

    query = "SELECT a.customer_shopify_id, b.shopify_product_id, count(b.shopify_product_id) FROM (SELECT tl_item_id, customer_shopify_id FROM `tetris-effect-203015.EDW_Netsuite.Transactions` WHERE partition_key > CAST('2015-08-01' AS DATE)) a JOIN `tetris-effect-203015.EDW_Netsuite.Items` b ON a.tl_item_id = b.item_id group by a.customer_shopify_id, b.shopify_product_id"

    df = client.query(
    query,
    job_config=job_config)  # API request - starts the query

    results = df.result() #wait for job completion


    for row in results:
        print(row)


if __name__ == '__main__':
    main()

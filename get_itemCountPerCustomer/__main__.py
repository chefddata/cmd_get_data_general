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

    query = "SELECT t.customer_shopify_id , i.shopify_product_id, count(*) FROM `tetris-effect-203015.EDW_Netsuite.Transactions` t, `tetris-effect-203015.EDW_Netsuite.Items` i WHERE t.transaction_type = 'Sales Order' and t.status != 'Closed' and t.partition_key > CAST('2015-08-01' AS DATE) and i.item_id = t.tl_item_id GROUP BY t.customer_shopify_id , i.shopify_product_id "

    df = client.query(
    query,
    job_config=job_config)  # API request - starts the query

    results = df.result() #wait for job completion


    for row in results:
        print(row)


if __name__ == '__main__':
    main()

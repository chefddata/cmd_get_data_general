# to communicate with Google BigQuery
from pandas.io import gbq
import sys


def main():
    print('loading data from BigQuery: Cusomer Shopify ID | Item Shopify ID | Count of Item')
<<<<<<< HEAD
    query = "SELECT tl_item_id, customer_shopify_id, count(tl_item_id) FROM `tetris-effect-203015.EDW_Netsuite.Transactions` WHERE partition_key > CAST('2015-08-01' AS DATE) group by customer_shopify_id, tl_item_id"
=======
    query = "SELECT tl_item_id AS item_id, customer_shopify_id AS cust_id, count(tl_item_id) AS count_iterms FROM `tetris-effect-203015.EDW_Netsuite.Transactions` WHERE partition_key > CAST("2015-08-01" AS DATE) group by customer_shopify_id, tl_item_id"
>>>>>>> 285ed753403af3dd4fb6bbd4a570771429809ed7

    df = gbq.read_gbq(query, project_id = "tetris-effect-203015")
    df.to_csv("itemCountPerCustomer.csv")


if __name__ == '__main__':
    main()

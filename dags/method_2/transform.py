from airflow.decorators import task
import pandas as pd
import logging

@task
def transform_accounting_unit_data_mart(sales_tx_t, sales_t):
    sales_tx_t = pd.read_json(sales_tx_t, orient="table")
    sales_t = pd.read_json(sales_t, orient="table")

    sales_tx_t = sales_tx_t[sales_tx_t["status"] == True][["id", "sales_id"]]

    sales_t["create_date"] = pd.to_datetime(sales_t["create_date"]).dt.strftime("%Y-%m-%d")

    df = sales_tx_t.merge(sales_t, left_on="sales_id", right_on="id", how="left")[["paid_price", "create_date"]]

    logging.info(f"Transformed table accounting_unit_data_mart is create sales_tx_t table and sales_t table")
    logging.info(f"Row number of transformed table accounting_unit_data_mart: {len(df.index)}")

    return df.to_json(orient="table")
    
@task
def transform_marketing_unit_data_mart(sales_tx_t, sales_t, store_t):
    sales_tx_t = pd.read_json(sales_tx_t, orient="table")
    sales_t = pd.read_json(sales_t, orient="table")
    store_t = pd.read_json(store_t, orient="table")

    sales_tx_t = sales_tx_t[sales_tx_t["status"] == True][["id", "sales_id", "store_id"]]
    sales_t["create_date"] = pd.to_datetime(sales_t["create_date"]).dt.strftime("%Y-%m-%d")

    df = sales_tx_t.merge(sales_t, left_on="sales_id", right_on="id", how="left") \
        .merge(store_t, left_on="store_id", right_on="id", how="left")
    df = df[["name", "status", "create_date", "paid_price"]]

    df = df.rename(columns={"name": "store_name", "status": "store_status", "create_date": "sales_date", "paid_price": "sales_price"})

    logging.info(f"Transformed table accounting_unit_data_mart is create sales_tx_t table, sales_t table and store_t table")
    logging.info(f"Row number of transformed table marketing_unit_data_mart: {len(df.index)}")

    return df.to_json(orient="table")

@task
def transform_customer_unit_data_mart(customer_t):
    return customer_t

transforms_tasks = {
    "data_marts.accounting_unit_data_mart": transform_accounting_unit_data_mart,
    "data_marts.marketing_unit_data_mart": transform_marketing_unit_data_mart,
    "data_marts.customer_unit_data_mart": transform_customer_unit_data_mart
}

from dummy.utils import *
import dummy.queries as queries
import pandas as pd
from sqlalchemy import create_engine, text, URL
import numpy as np

def create_sales_t_as_pandas_dataframe(row_number, start_year, price_start, price_stop, digit):
    return pd.DataFrame({
        'id': generate_id_values(row_number),
        'create_date': generate_date_values(start_year=start_year, row_number=row_number),
        'paid_price': generate_price_values(row_number, price_start, price_stop, digit)
    })

def create_customer_t_as_pandas_dataframe(row_number, name_prefix, start_year):
    return pd.DataFrame({
        "id": generate_id_values(row_number),
        "name": generate_name_values(row_number, name_prefix),
        "city": generate_city_values(row_number),
        "create_date": generate_date_values(start_year=start_year, row_number=row_number)
    })

def create_store_t_as_pandas_dataframe(row_number, name_prefix, start_year):
    return pd.DataFrame({
        "id": generate_id_values(row_number),
        "name": generate_name_values(row_number, name_prefix),
        "live_date": generate_date_values(start_year, row_number),
        "status": generate_status_values(row_number)
    }).astype({"status": bool})

def create_sales_tx_t_as_pandas_dataframe(row_number, sales_id_array, store_id_array):
    return pd.DataFrame({
        "id": generate_id_values(row_number),
        "status": generate_status_values(row_number),
        "sales_id": [np.random.choice(sales_id_array) for i in range(row_number)],
        "store_id": [np.random.choice(store_id_array) for i in range(row_number)]
    }).astype({"status": bool})


def create_dummy_data(db_params, schema, customer_count, store_count, sales_count, if_exits):
    engine = create_engine(URL.create(
        "postgresql+psycopg2",
        **db_params
    ))
    conn = engine.connect()
    conn.execute(text(queries.customer_t.replace("{{schema}}", schema)))
    conn.execute(text(queries.store_t.replace("{{schema}}", schema)))
    conn.execute(text(queries.sales_t.replace("{{schema}}", schema)))
    conn.execute(text(queries.sales_tx_t.replace("{{schema}}", schema)))
    conn.close()

    # customer_t table dummy data
    customer_t = create_customer_t_as_pandas_dataframe(
        row_number=customer_count,
        name_prefix="Müşteri",
        start_year=2021
    )
    customer_t.to_sql("customer_t", engine, if_exists=if_exits, index=False, schema=schema)
    print("Customer append to db")


    # store_t table dummy data
    store_t = create_store_t_as_pandas_dataframe(
        row_number=store_count,
        name_prefix="Mağaza",
        start_year=2021
    )
    store_t.to_sql("store_t", engine, if_exists=if_exits, index=False, schema=schema)
    print("Store append to db")


    # sales_t table dummy data
    sales_t = create_sales_t_as_pandas_dataframe(
        row_number=sales_count,
        start_year=2022,
        price_start=1,
        price_stop=100,
        digit=2
    )
    sales_t.to_sql("sales_t", engine, if_exists=if_exits, index=False, schema=schema)
    print("Sales append to db")
    

    # sales_tx_t table dummy data
    sales_tx_t = create_sales_tx_t_as_pandas_dataframe(
        sales_count,
        sales_id_array=sales_t["id"].to_numpy(),
        store_id_array=store_t[store_t['status'] == 1]["id"].to_numpy()
    )
    sales_tx_t.to_sql("sales_tx_t", engine, if_exists=if_exits, index=False, schema=schema)
    print("Sales Transaction append to db")
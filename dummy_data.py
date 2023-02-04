import pandas as pd
import numpy as np
from random import randint, random
from datetime import datetime
from sqlalchemy import create_engine

def random_date_generator(start_year):
    return datetime(start_year, randint(1, 12), randint(1, 28), randint(0, 23), randint(1, 59), randint(1, 59), randint(0, 999999))

def generate_id_values(row_number):
    return np.arange(1, row_number + 1, 1)

def generate_date_values(start_year, row_number):
    return [random_date_generator(start_year) for i in range(row_number)]

def generate_price_values(row_number, price_start, price_stop, digit):
    return np.round(np.random.rand(row_number) * price_stop + price_start, digit)

def generate_status_values(row_number):
    return [1 if random() < 0.8 else 0 for i in range(row_number)]

def generate_name_values(row_number, name_prefix):
    numbers = np.arange(1, row_number + 1).astype(str)
    names = np.array([name_prefix for i in range(row_number)])
    return np.core.defchararray.add(names, numbers)

def generate_city_values(row_number):
    cities = np.array(["İstanbul", "İzmir", "Ankara", "Erzurum", "Muğla", "Adana", "Antalya"])
    return [np.random.choice(cities) for i in range(row_number)]

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


if __name__ == '__main__':

    customer_count = 250
    store_count = 20
    sales_count = 75000
    if_exits = "append" # fail, replace

    user = "source"
    password = "source"
    database = "source"
    host = "localhost"
    port = "8000"

    engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}')
    schema = "public"

    sales_t = create_sales_t_as_pandas_dataframe(
        row_number=sales_count,
        start_year=2022,
        price_start=1,
        price_stop=100,
        digit=2
    )

    customer_t = create_customer_t_as_pandas_dataframe(
        row_number=customer_count,
        name_prefix="Müşteri",
        start_year=2021
    )

    store_t = create_store_t_as_pandas_dataframe(
        row_number=store_count,
        name_prefix="Mağaza",
        start_year=2021
    )

    sales_tx_t = create_sales_tx_t_as_pandas_dataframe(
        sales_count,
        sales_id_array=sales_t["id"].to_numpy(),
        store_id_array=store_t[store_t['status'] == 1]["id"].to_numpy()
    )

    customer_t.to_sql("customer_t", engine, if_exists=if_exits, index=False, schema=schema)
    print("Customer append to db")

    store_t.to_sql("store_t", engine, if_exists=if_exits, index=False, schema=schema)
    print("Store append to db")

    sales_t.to_sql("sales_t", engine, if_exists=if_exits, index=False, schema=schema)
    print("Sales append to db")
    
    sales_tx_t.to_sql("sales_tx_t", engine, if_exists=if_exits, index=False, schema=schema)
    print("Sales Transaction append to db")

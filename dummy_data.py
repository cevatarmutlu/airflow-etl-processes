from dummy.data import create_dummy_data

if __name__ == '__main__':

    customer_count = 2500
    store_count = 50
    sales_count = 20000
    if_exits = "append" # fail, replace
    schema = "public"

    db_params = {
        "username": "source",
        "password": "source",
        "database": "source",
        "host": "localhost",
        "port": "8000"
    }

    create_dummy_data(db_params, schema, customer_count, store_count, sales_count, if_exits)

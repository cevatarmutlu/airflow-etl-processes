from airflow.decorators import task
import pandas as pd
from airflow.utils.task_group import TaskGroup


@task
def truncate_or_create(engine, schema, table, columns):
    try:
        engine.execute(f"TRUNCATE TABLE {schema}.{table}")
    except:
        print("Table not created")
        engine.execute(f"""CREATE TABLE {schema}.{table} (
                            {columns}
                        );"""
        )
    return True

@task
def extract_transform(engine, query, truncate_or_create_task):

    data = pd.read_sql(
        sql=query,
        con=engine
    )

    return data.to_json(orient="table")

@task
def load(data, engine, write_schema, write_table):

    # Convert JSON Data to Pandas DataFrame
    df = pd.read_json(data, orient="table")

    # Insert Data to Database
    df.to_sql(write_table, engine, index=False, if_exists="append", schema=write_schema)

    print("Insert operation is successfull")

@task
def healt_check(engine):

    for table in ["public.sales_tx_t", "public.sales_t", "public.store_t", "data_marts.accounting_unit_data_mart", "data_marts.marketing_unit_data_mart"]:

        data = pd.read_sql(
            sql=f"SELECT * FROM {table}",
            con=engine
        )

        logging.info(f"{table} rows: {len(data.index)}")
    
    process = subprocess.Popen(["df", "-H"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    

    out, err = process.communicate()
    logging.info("\n" + out.decode("utf-8"))

def etl_task_group(key, value, engine):
    with TaskGroup(key) as tg:
        truncate_or_create_task = truncate_or_create(engine, value.get("write_schema"), value.get("write_table"), value.get("columns"))
        data = extract_transform(engine, value.get("query"), truncate_or_create_task)
        load(data, engine, value.get("write_schema"), value.get("write_table"))
    
    return tg
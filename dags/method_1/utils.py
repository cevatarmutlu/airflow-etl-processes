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
def extract_transform(engine, query):

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
def healt_check():
    import logging
    import subprocess
    
    process = subprocess.Popen(["df", "-H"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    

    out, err = process.communicate()
    logging.info("\n" + out.decode("utf-8"))


def etl_task_group(key, value, engine_source, engine_target):
    with TaskGroup(key) as tg:
        truncate_or_create_task = truncate_or_create(engine_target, value.get("write_schema"), value.get("write_table"), value.get("columns"))
        data = extract_transform(engine_source, value.get("query"))
        load_task = load(data, engine_target, value.get("write_schema"), value.get("write_table"))

        truncate_or_create_task >> data >> load_task
    
    return tg

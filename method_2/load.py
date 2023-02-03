from airflow.decorators import dag, task_group, task
import pandas as pd
import logging
from airflow.models import Variable

@task
def load_accounting_unit_data_mart(tranformed_data, engine, write_schema, write_table):
    df = pd.read_json(tranformed_data, orient="table")
    #df["create_date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Insert Data to Database
    df.to_sql("accounting_unit_data_mart", engine, index=False, if_exists="append", schema=write_schema)

    logging.info("Load operation is successfull: accounting_unit_data_mart")
    

@task
def load_marketing_unit_data_mart(tranformed_data, engine, write_schema, write_table):
    df = pd.read_json(tranformed_data, orient="table")
    #df["create_date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Insert Data to Database
    df.to_sql("marketing_unit_data_mart", engine, index=False, if_exists="append", schema=write_schema)

    logging.info("Load operation is successfull: marketing_unit_data_mart")


def get_laod_task_as_dict(transform_task, engine):
    load_task = []
    for table_x in Variable.get("method2_transformed_table_names").split(","):


        @task(task_id=f"load_{table_x.strip()}")
        def load(tranformed_data, engine, write_schema, write_table):
            df = pd.read_json(tranformed_data, orient="table")

            # Insert Data to Database
            df.to_sql(write_table, engine, index=False, if_exists="append", schema=write_schema)

            logging.info(f"Load operation is successfull: {write_table}")
                
        load_task.append(load(transform_task.get(table_x.strip()), engine, "data_marts", table_x.strip()))
    
    return load_task

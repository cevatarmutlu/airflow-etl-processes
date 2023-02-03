from airflow.decorators import dag, task_group, task
from datetime import datetime
import pandas as pd
from method_2.extract import get_extract_tasks_as_dict
from method_2.transform import *
from method_2.load import get_laod_task_as_dict
import subprocess
import logging


@task
def start():
    process = subprocess.Popen(["df", "-H"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    

    out, err = process.communicate()
    logging.info("\n" + out.decode("utf-8"))



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



@dag(
    dag_id="Iyzico_etl_method2",
    schedule=None, #"*/20 * * * *", 
    tags=["Iyzico", "etl"],
    start_date=datetime(2020, 2, 2)
)
def dag():
    from airflow.hooks.base import BaseHook
    from sqlalchemy import create_engine
    from airflow.models import Variable

    db = BaseHook.get_connection(Variable.get("iyzico_etl_source_connection_id"))
    engine = create_engine(f'postgresql+psycopg2://{db.login}:{db.password}@{db.host}:{db.port}/{db.schema}')
    

    start_task = start()

    extract_tasks_dict = get_extract_tasks_as_dict()

    # Transform
    accounting = transform_accounting_unit_data_mart(**extract_tasks_dict)
    marketing = transform_marketing_unit_data_mart(**extract_tasks_dict)
    transform_task = {
        "accounting_unit_data_mart": accounting,
        "marketing_unit_data_mart": marketing
    }
    
    load_task = get_laod_task_as_dict(transform_task, engine)

    start_task >> list(extract_tasks_dict.values())
    load_task >> healt_check(engine)

dag()

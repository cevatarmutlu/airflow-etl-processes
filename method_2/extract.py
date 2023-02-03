from airflow.decorators import dag, task_group, task
import pandas as pd
import logging
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine
from airflow.models import Variable


def get_extract_tasks_as_dict():
    db = BaseHook.get_connection(Variable.get("iyzico_etl_source_connection_id"))
    engine = create_engine(f'postgresql+psycopg2://{db.login}:{db.password}@{db.host}:{db.port}/{db.schema}')

    extract_tasks = {}

    for table_x in Variable.get("method2_extract_tables").split(","):
        @task(task_id=f"extract_{table_x.strip()}")
        def extract(table, engine):
            data = pd.read_sql(
                sql=f"SELECT * FROM {table.strip()}",
                con=engine
            )

            logging.info(f"Read table: {table.strip()}")
            logging.info(f"Row number of {table.strip()}: {len(data.index)}")

            return data.to_json(orient="table")
        
        extract_tasks[table_x.strip()] = extract(table_x, engine)
    
    return extract_tasks

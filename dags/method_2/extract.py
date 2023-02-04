from airflow.decorators import task
import pandas as pd
import logging
from airflow.models import Variable


def get_extract_tasks_as_dict(engine):
    
    extract_tasks = {}

    for table_name in Variable.get("method2_extract_tables").split(","):
        @task(task_id=f"extract_{table_name.strip().replace('.', '_')}")
        def extract(table, engine):
            data = pd.read_sql(
                sql=f"SELECT * FROM {table}",
                con=engine
            )

            logging.info(f"Read table: {table}")
            logging.info(f"Row number of {table}: {len(data.index)}")

            return data.to_json(orient="table")
        
        extract_tasks[table_name.strip().replace(".", "_")] = extract(table_name.strip(), engine)
    
    return extract_tasks

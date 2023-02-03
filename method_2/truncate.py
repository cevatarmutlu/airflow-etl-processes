from airflow.decorators import task
import pandas as pd
import logging
from airflow.models import Variable

truncate_tasks = {}

for table_name in Variable.get("method2_transformed_table_names").split(","):
    @task(task_id=f"truncate_{table_name.strip().replace('.', '_')}")
    def truncate(engine, schema_table, data):
        schema = schema_table.split(".")[0]
        table = schema_table.split(".")[1]
        try:
            engine.execute(f"TRUNCATE TABLE {schema}.{table}")
            logging.info(f"Truncate operation is successfull: {schema}.{table}")
        except:
            logging.info(f"Truncate operation is unsuccessfull: {schema}.{table}")

        return data

    truncate_tasks[table_name.strip()] = truncate

        

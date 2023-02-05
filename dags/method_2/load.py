from airflow.decorators import task
import pandas as pd
import logging
from airflow.models import Variable

load_tasks_dict = {}

for table_name in Variable.get("method2_transformed_table_names").split(","):
    @task(task_id=f"load_{table_name.strip().replace('.', '_')}")
    def load(tranformed_data, engine, schema_table):
        schema = schema_table.split(".")[0]
        table = schema_table.split(".")[1]
        df = pd.read_json(tranformed_data, orient="table")

        # Insert Data to Database
        df.to_sql(table, engine, index=False, if_exists="append", schema=schema)

        logging.info("Load operation is successfull: marketing_unit_data_mart")

    load_tasks_dict[table_name.strip()] = load

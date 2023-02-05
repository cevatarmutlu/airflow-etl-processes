from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
import json
from datetime import datetime

from method_2.extract import get_extract_tasks_results_as_dict
from method_2.transform import transform_tasks_dict
from method_2.load import load_tasks_dict
from method_2.utils import *
from method_2.truncate import truncate_tasks_dict

@dag(
    dag_id="etl_method2",
    schedule="*/20 * * * *", 
    tags=["etl", "method2"],
    start_date=datetime(2023, 2, 2)
)
def dag():

    engine_source = PostgresHook(Variable.get("source_connection")).get_sqlalchemy_engine()
    engine_target = PostgresHook(Variable.get("target_connection")).get_sqlalchemy_engine()
    

    start_task = start()

    extract_tasks_result_dict = get_extract_tasks_results_as_dict(engine_source)

    tasks = []
    for load_table, load_table_req_source_tables in json.loads(Variable.get("method2_transform_mapping")).items():

        transform_task = transform_tasks_dict[load_table.strip()]
        transformed_data = transform_task(
            *[extract_tasks_result_dict[i.strip().replace(".", "_")] for i in load_table_req_source_tables.split(",")]
        )

        truncate_task = truncate_tasks_dict[load_table.strip()]
        truncate_task_result = truncate_task(engine_target, load_table.strip(), transformed_data)

        load_task = load_tasks_dict[load_table.strip()]
        load_task_result = load_task(truncate_task_result, engine_target, load_table.strip())
        
        tasks.append(load_task_result)

    
    healt = healt_check()

    start_task >> list(extract_tasks_result_dict.values())
    tasks >> healt >> EmptyOperator(task_id="end")

dag()
